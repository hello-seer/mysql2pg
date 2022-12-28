"""Async MySQL driver

Would use asyncio, but streaming does not work. (cursor.execute() waits until the statement
is completed.)
"""

import asyncio
import concurrent.futures
import contextlib

import mysql.connector


class Pool:
    def __init__(self, name, size, executor, **kwargs):
        self._name = name
        self._size = size
        self._executor = executor
        self._options = kwargs

    @contextlib.asynccontextmanager
    async def acquire(self):
        loop = asyncio.get_running_loop()
        with contextlib.closing(
            await loop.run_in_executor(
                self._executor,
                lambda: mysql.connector.connect(
                    pool_name=self._name, pool_size=self._size, **self._options
                ),
            )
        ) as conn:
            yield Connection(conn, self._executor)


class Connection:
    def __init__(self, connection, executor):
        self._conn = connection
        self._executor = executor

    @contextlib.asynccontextmanager
    async def cursor(self):
        loop = asyncio.get_running_loop()
        with contextlib.closing(
            await loop.run_in_executor(self._executor, self._conn.cursor)
        ) as cur:
            yield Cursor(cur, self._executor)


class Cursor:
    def __init__(self, cursor, executor):
        self._cursor = cursor
        self._executor = executor

    def __aiter__(self):
        return self._rows().__aiter__()

    async def execute(self, *args):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._cursor.execute, *args)

    async def _rows(self):
        loop = asyncio.get_running_loop()
        while True:
            rows = await loop.run_in_executor(
                self._executor, self._cursor.fetchmany, 500
            )
            if not rows:
                break
            for row in rows:
                yield row


_pool_count = 0


@contextlib.asynccontextmanager
async def create_pool(size, **kwargs):
    global _pool_count
    name = f"pool{_pool_count}"
    _pool_count += 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=size) as executor:
        yield Pool(name, size, executor, **kwargs)
