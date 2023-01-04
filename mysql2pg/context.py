import asyncio
import contextlib
import sys
import typing

T = typing.TypeVar("T")


@contextlib.asynccontextmanager
async def lazy_asynccontext(
    fn: typing.Callable[[], typing.AsyncContextManager[T]]
) -> typing.AsyncIterator[T]:
    async with fn() as value:
        yield value


@contextlib.contextmanager
def sync_contextmanager(
    loop: asyncio.AbstractEventLoop,
    context: typing.AsyncContextManager[T],
) -> typing.Iterator[T]:
    """Converts AsyncContextManager to ContextManager"""
    try:
        yield loop.run_until_complete(context.__aenter__())
    finally:
        loop.run_until_complete(context.__aexit__(*sys.exc_info()))
