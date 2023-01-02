import contextlib


@contextlib.asynccontextmanager
async def lazy_asynccontext(fn):
    async with fn() as value:
        try:
            yield value
        except:
            import traceback

            traceback.print_exc()
            raise
