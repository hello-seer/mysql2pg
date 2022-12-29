import asyncio
import contextlib
import os

import aiomysql
import asyncpg
from pytest import mark

from mysql2pg import mysql


@contextlib.asynccontextmanager
async def pg_connect(*args, **kwargs):
    conn = await asyncpg.connect(*args, **kwargs)
    try:
        yield conn
    finally:
        await conn.close()


@mark.asyncio
async def test_migrate():
    async with aiomysql.connect(**mysql.mysql_params()) as conn, conn.cursor() as cur:
        await cur.execute("DROP DATABASE IF EXISTS mysql2pg_test")
        await cur.execute("CREATE DATABASE mysql2pg_test")
    async with aiomysql.connect(
        **mysql.mysql_params(), db="mysql2pg_test"
    ) as conn, conn.cursor() as cur:
        await cur.execute("CREATE TABLE example (id serial, time datetime(6))")
        await cur.execute("INSERT INTO example (time) VALUES (now(6)), (now(6))")
    async with pg_connect(database="postgres") as conn:
        await conn.execute("DROP DATABASE IF EXISTS mysql2pg_test")
        await conn.execute("CREATE DATABASE mysql2pg_test")
    async with pg_connect(database="mysql2pg_test") as conn:
        await conn.execute("CREATE TABLE example (id serial, time timestamptz)")
    env = dict(os.environ)
    env["MYSQL_DATABASE"] = "mysql2pg_test"
    env["PGDATABASE"] = "mysql2pg_test"
    proc = await asyncio.create_subprocess_exec("mysql2pg", env=env)
    await proc.communicate()
    code = await proc.wait()
    assert code == 0
