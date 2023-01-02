import asyncio
import contextlib
import sys
import typing

T = typing.TypeVar("T")

async def run_all(tasks: typing.Iterable):
    tasks = tuple(tasks)
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@contextlib.contextmanager
def sync_contextmanager(
    context: typing.AsyncContextManager[T], loop
) -> typing.ContextManager[T]:
    try:
        yield loop.run_until_complete(context.__aenter__())
    finally:
        loop.run_until_complete(context.__aexit__(*sys.exc_info()))
