import asyncio
import contextlib
import os
import warnings

import aiomysql
import asyncpg
from pytest import mark

from mysql2pg import mysql

warnings.filterwarnings("ignore", module="aiomysql")


@contextlib.asynccontextmanager
async def _mysql_database(params, name="mysql2pg_test"):
    async with aiomysql.connect(**params) as conn, conn.cursor() as cur:
        cur._defer_warnings = True
        await cur.execute(f"DROP DATABASE IF EXISTS {name}")
        await cur.execute(f"CREATE DATABASE {name}")
        try:
            yield name
        finally:
            try:
                await cur.execute(f"DROP DATABASE {name}")
            except Exception:
                pass


@contextlib.asynccontextmanager
async def _pg_connect(*args, **kwargs):
    conn = await asyncpg.connect(*args, **kwargs)
    try:
        yield conn
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def _pg_database(params, name="mysql2pg_test"):
    async with _pg_connect(database="postgres") as conn:
        await conn.execute(f"DROP DATABASE IF EXISTS {name}")
        await conn.execute(f"CREATE DATABASE {name}")
        try:
            yield name
        finally:
            try:
                await conn.execute(f"DROP DATABASE {name}")
            except Exception:
                pass


@mark.asyncio
async def test_migrate():
    async with _mysql_database(mysql.conn_params()) as mysql_db, _pg_database(
        {}
    ) as pg_db:
        async with aiomysql.connect(
            **mysql.conn_params(), autocommit=True, db=mysql_db
        ) as conn, conn.cursor() as cur:
            await cur.execute("CREATE TABLE example (id serial, time datetime(6))")
            await cur.execute("INSERT INTO example (time) VALUES (now(6)), (now(6))")
        async with _pg_connect(database=pg_db) as conn:
            await conn.execute("CREATE TABLE example (id serial, time timestamptz)")
        env = {**os.environ, "MYSQL_DATABASE": mysql_db, "PGDATABASE": pg_db}
        proc = await asyncio.create_subprocess_exec("mysql2pg", env=env)
        code = await proc.wait()
        assert code == 0
        async with _pg_connect(database=pg_db) as conn:
            result = await conn.fetch("TABLE example")
            assert len(result) == 2
