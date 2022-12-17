import asyncio
import os
import sys
import time

import aiomysql
import asyncpg

from .async_ import Throttle, run_parallel


async def _count_items(generator, output):
    count = 0
    async for item in generator:
        count += 1
        yield item
    output.append(count)


async def _copy_table(mysql_pool: aiomysql.Pool, pg_pool: asyncpg.Pool, table: str):
    start = time.process_time()
    print(f"Copying table {table}", file=sys.stderr)
    await asyncio.sleep(1)
    async with mysql_pool.acquire() as mysql_conn, mysql_conn.cursor() as mysql_cur, pg_pool.acquire() as pg_conn:
        await mysql_cur.execute(f"SHOW columns FROM `{table}`")
        columns = []
        async for row in mysql_cur:
            columns.append(row[0])
        await mysql_cur.execute(
            f"SELECT {', '.join(f'`{column}`' for column in columns)} FROM `{table}`"
        )

        await pg_conn.execute(f"TRUNCATE {table}")
        rows = []
        await pg_conn.copy_records_to_table(
            table, records=_count_items(mysql_cur, rows), columns=columns
        )
    end = time.process_time()
    print(
        f"Copied {rows[0]} rows to table {table} ({end - start:.2f}s)", file=sys.stderr
    )


async def _pg_init(conn):
    await conn.execute("SET session_replication_role = replica")


def _mysql_params():
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


async def migrate():
    async with aiomysql.create_pool(
        **_mysql_params()
    ) as mysql_pool, asyncpg.create_pool(init=_pg_init) as pg_pool:
        async with mysql_pool.acquire() as mysql_conn, mysql_conn.cursor() as mysql_cur:
            await mysql_cur.execute("SHOW TABLES")
            tables = []
            async for row in mysql_cur:
                tables.append(row[0])
            # tables = tuple(row[0] async for row in mysql_cur)

        throttle = Throttle(10)
        print(f"Copying {len(tables)} tables", file=sys.stderr)
        await run_parallel(
            throttle.throttle(_copy_table(mysql_pool, pg_pool, table))
            for table in tables
        )
