import logging
import typing

import aiomysql
import asyncpg
from aiomysql import cursors

from .async_ import Throttle, run_parallel
from .function_ import identity
from .mysql import mysql_params
from .time_ import Timer


def _converter(namespace: str, name: str) -> typing.Callable:
    if (namespace, name) == ("pg_catalog", "bool"):
        return bool
    return identity


async def _convert(cur: aiomysql.Cursor, converters: typing.Iterable[typing.Callable]):
    async for row in cur:
        yield tuple(converter(value) for value, converter in zip(row, converters))


async def _copy_table(mysql_pool: aiomysql.Pool, pg_pool: asyncpg.Pool, table: str):
    try:
        timer = Timer()
        logging.info(f"Copying table {table}")
        async with mysql_pool.acquire() as mysql_conn, mysql_conn.cursor() as mysql_cur, pg_pool.acquire() as pg_conn:
            await mysql_cur.execute(f"SHOW columns FROM `{table}`")
            columns = []
            async for row in mysql_cur:
                columns.append(row[0])
            await mysql_cur.execute(
                f"SELECT {', '.join(f'`{column}`' for column in columns)} FROM `{table}`"
            )

            types = await pg_conn.fetch(
                """
SELECT pn.nspname, pt.typname
FROM
    unnest($2::text[]) WITH ORDINALITY AS d (name, ordinal)
    JOIN pg_attribute AS pa ON d.name = pa.attname
    JOIN pg_type AS pt ON pa.atttypid = pt.oid
    JOIN pg_namespace AS pn ON pt.typnamespace = pn.oid
WHERE pa.attrelid = $1::regclass
ORDER BY d.ordinal
                """,
                table,
                columns,
            )
            if len(columns) != len(types):
                raise RuntimeError("Missing columns from PostgreSQL")
            data = _convert(mysql_cur, tuple(_converter(*t) for t in types))

            count = 0

            async def log_records():
                nonlocal count
                async for item in data:
                    count += 1
                    if not count % (1000 * 50):
                        logging.debug(
                            f"Copied {count} rows to table {table}... ({timer.elapsed():.2f}s)"
                        )
                    yield item

            await pg_conn.copy_records_to_table(
                table, records=log_records(), columns=columns
            )
        logging.info(
            f"Copied {count} rows to table {table} ({timer.elapsed():.2f}s)",
        )
    except Exception as e:
        logging.error(f"Failed to copy table {table}: {e}")
        raise

    async with pg_pool.acquire() as conn:
        sequences = await conn.fetch(
            """
SELECT *
FROM
    (
        SELECT a.attname, pg_get_serial_sequence(c.relname, a.attname) AS sequence
        FROM
            pg_class AS c
            JOIN pg_attribute AS a ON c.oid = a.attrelid
        WHERE c.oid = $1::regclass AND c.relkind = 'r' AND a.atttypid <> 0 AND 0 < a.attnum
    ) AS t
WHERE sequence IS NOT NULL
            """,
            table,
        )
    for column, sequence in sequences:
        try:
            timer = Timer()
            logging.info(f"Resetting sequence {sequence}")
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    f"SELECT setval($1::regclass, 1 + coalesce((SELECT max({column}) FROM {table}), 0), false)",
                    sequence,
                )
            logging.info(
                f"Reset sequence {sequence} ({timer.elapsed():.2f}s)",
            )
        except Exception as e:
            logging.error(f"Failed to reset sequence {sequence}: {e}")


async def _mysql_tables(pool: aiomysql.Pool) -> typing.Iterable[str]:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SHOW TABLES")
        tables = []
        async for row in cur:
            tables.append(row[0])
    logging.info(f"Found {len(tables)} tables")
    return tables


async def _pg_truncate(pool: asyncpg.Pool, tables: typing.Iterable[str]):
    # need to all tables at once because https://www.postgresql.org/message-id/15657-f94bb6e3ad28e1e2%40postgresql.org
    logging.info(f"Truncating {len(tuple(tables))} tables")
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {', '.join(tables)}")


async def migrate(
    parallelism: int, pg_search_path: str | None, tables: typing.Iterable[str] | None
):
    pg_server_settings = {
        "application_name": "mysql2pg",
        "session_replication_role": "replica",
    }
    if pg_search_path:
        pg_server_settings["search_path"] = pg_search_path
    async with aiomysql.create_pool(
        cursorclass=cursors.SSCursor, minsize=0, maxsize=parallelism, **mysql_params()
    ) as mysql_pool, asyncpg.create_pool(
        min_size=0, max_size=parallelism, server_settings=pg_server_settings
    ) as pg_pool:
        if tables is None:
            tables = await _mysql_tables(mysql_pool)

        await _pg_truncate(pg_pool, tables)

        throttle = Throttle(parallelism)
        await run_parallel(
            throttle.throttle(_copy_table(mysql_pool, pg_pool, table))
            for table in tables
        )
