import concurrent.futures
import logging
import os
import time
import typing

import asyncpg

from .async_ import Throttle, run_parallel
from .iterator import async_count_elements
from .mysql import create_pool, Pool, Cursor


def _converter(namespace: str, name: str):
    if (namespace, name) == ("pg_catalog", "bool"):
        return bool
    return lambda x: x


async def _convert(cur: Cursor, converters: typing.Sequence[typing.Callable]):
    async for row in cur:
        yield tuple(converter(value) for value, converter in zip(row, converters))


async def _copy_table(mysql_pool: Pool, pg_pool: asyncpg.Pool, table: str):
    try:
        start = time.process_time()
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
                        end = time.process_time()
                        logging.debug(
                            f"Copied {count} rows to table {table}... ({end - start:.2f}s)"
                        )
                    yield item

            await pg_conn.copy_records_to_table(
                table, records=log_records(), columns=columns
            )
        end = time.process_time()
        logging.info(
            f"Copied {count} rows to table {table} ({end - start:.2f}s)",
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
            start = time.process_time()
            logging.info(f"Resetting sequence {sequence}")
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    f"SELECT setval($1::regclass, 1 + coalesce(0, (SELECT max({column}) FROM {table})), false)",
                    sequence,
                )
            end = time.process_time()
            logging.info(
                f"Reset sequence {sequence} ({end - start:.2f}s)",
            )
        except Exception as e:
            logging.error(f"Failed to reset sequence {sequence}: {e}")


def _mysql_params() -> typing.Dict[str, typing.Any]:
    params = {}
    try:
        params["db"] = os.environ["MYSQL_DATABASE"]
    except KeyError:
        pass
    try:
        params["host"] = os.environ["MYSQL_HOST"]
    except KeyError:
        pass
    try:
        params["password"] = os.environ["MYSQL_PASSWORD"]
    except KeyError:
        pass
    try:
        params["port"] = int(os.environ["MYSQL_PORT"])
    except KeyError:
        pass
    try:
        params["user"] = os.environ["MYSQL_USER"]
    except KeyError:
        pass
    return params


async def _mysql_tables(pool: Pool) -> typing.Tuple[str]:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SHOW TABLES")
        tables = []
        async for row in cur:
            tables.append(row[0])
    logging.info(f"Found {len(tables)} tables")
    return tables


async def _pg_truncate(pool: asyncpg.Pool, tables: typing.Tuple[str]):
    # need to all tables at once because https://www.postgresql.org/message-id/15657-f94bb6e3ad28e1e2%40postgresql.org
    logging.info(f"Truncating {len(tables)} tables")
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {', '.join(tables)}")


async def migrate(
    parallelism: int, pg_search_path: str | None, tables: typing.Sequence[str] | None
):
    pg_server_settings = {
        "application_name": "mysql2pg",
        "session_replication_role": "replica",
    }
    if pg_search_path:
        pg_server_settings["search_path"] = pg_search_path
    async with create_pool(
        parallelism, **_mysql_params()
    ) as mysql_pool, asyncpg.create_pool(server_settings=pg_server_settings) as pg_pool:
        if tables is None:
            tables = await _mysql_tables(mysql_pool)

        await _pg_truncate(pg_pool, tables)

        throttle = Throttle(parallelism)
        await run_parallel(
            throttle.throttle(_copy_table(mysql_pool, pg_pool, table))
            for table in tables
        )
