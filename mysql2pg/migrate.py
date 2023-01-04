import asyncio
import logging
import typing
from concurrent import futures

import aiomysql
import asyncpg
from aiomysql import cursors

from mysql2pg import async_, context, function, multiprocess, mysql, time

logger = logging.getLogger(__name__)


def _converter(namespace: str, name: str) -> typing.Callable[[typing.Any], typing.Any]:
    if (namespace, name) == ("pg_catalog", "bool"):
        return bool
    return function.identity


async def _convert(cur: aiomysql.Cursor, converters: typing.Iterable[typing.Callable]):
    async for row in cur:
        yield tuple(converter(value) for value, converter in zip(row, converters))


async def _copy_table(mysql_pool: aiomysql.Pool, pg_pool: asyncpg.Pool, table: str):
    try:
        timer = time.Timer()
        logger.info(f"Copying table {table}")
        async with typing.cast(
            typing.AsyncContextManager[aiomysql.Connection], mysql_pool.acquire()
        ) as mysql_conn, typing.cast(
            typing.AsyncContextManager[aiomysql.Cursor],
            mysql_conn.cursor(cursors.SSCursor),
        ) as mysql_cur, typing.cast(
            typing.AsyncContextManager[asyncpg.Connection], pg_pool.acquire()
        ) as pg_conn:
            await mysql_cur.execute(f"SHOW columns FROM `{table}`")
            columns = [row[0] async for row in mysql_cur]
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
                        logger.debug(
                            f"Copied {count} rows to table {table}... ({timer.elapsed():.2f}s)"
                        )
                    yield item

            await pg_conn.copy_records_to_table(
                table, records=log_records(), columns=columns
            )
        logger.info(
            f"Copied {count} rows to table {table} ({timer.elapsed():.2f}s)",
        )
    except Exception as e:
        logger.error(f"Failed to copy table {table}: {e}")
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
            timer = time.Timer()
            logger.info(f"Resetting sequence {sequence}")
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    f"SELECT setval($1::regclass, 1 + coalesce((SELECT max({column}) FROM {table}), 0), false)",
                    sequence,
                )
            logger.info(
                f"Reset sequence {sequence} ({timer.elapsed():.2f}s)",
            )
        except Exception as e:
            logger.error(f"Failed to reset sequence {sequence}: {e}")


async def _mysql_tables(pool: aiomysql.Pool) -> typing.Iterable[str]:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SHOW TABLES")
        tables = [table async for table, in cur]
    logger.info(f"Found {len(tables)} tables")
    return tables


async def _pg_truncate(pool: asyncpg.Pool, tables: typing.Iterable[str]):
    # need to all tables at once because https://www.postgresql.org/message-id/15657-f94bb6e3ad28e1e2%40postgresql.org
    logger.info(f"Truncating {len(tuple(tables))} tables")
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {', '.join(tables)}")


_mysql_pool: multiprocess.Context[aiomysql.Pool] = multiprocess.Context()
_pg_pool: multiprocess.Context[asyncpg.Pool] = multiprocess.Context()

_loop = asyncio.new_event_loop()


def _copy(table: str):
    _loop.run_until_complete(_copy_table(_mysql_pool.value, _pg_pool.value, table))


async def migrate(
    parallelism: int, pg_search_path: str | None, tables: typing.Iterable[str] | None
):
    loop = asyncio.get_running_loop()

    pg_server_settings = {
        "application_name": "mysql2pg",
        "session_replication_role": "replica",
    }
    if pg_search_path:
        pg_server_settings["search_path"] = pg_search_path
    async with aiomysql.create_pool(
        minsize=0, maxsize=1, **mysql.conn_params()
    ) as mysql_pool, asyncpg.create_pool(
        min_size=0, max_size=1, server_settings=pg_server_settings
    ) as pg_pool:
        if tables is None:
            tables = await _mysql_tables(mysql_pool)

        await _pg_truncate(pg_pool, tables)

        mysql_initializer = _mysql_pool.initializer(
            context.sync_contextmanager(
                _loop,
                context.lazy_asynccontext(
                    lambda: aiomysql.create_pool(
                        minsize=0,
                        maxsize=1,
                        **mysql.conn_params(),
                    )
                ),
            )
        )
        pg_initializer = _pg_pool.initializer(
            context.sync_contextmanager(
                _loop,
                context.lazy_asynccontext(
                    lambda: asyncpg.create_pool(
                        min_size=0, max_size=1, server_settings=pg_server_settings
                    )
                ),
            )
        )

        def initializer():
            mysql_initializer()
            pg_initializer()

        with futures.ProcessPoolExecutor(
            max_workers=parallelism, initializer=initializer
        ) as pool:
            await async_.run_all(
                loop.run_in_executor(pool, _copy, table) for table in tables
            )
