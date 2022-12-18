import asyncio


class Throttle:
    def __init__(self, max: int):
        self._semaphore = asyncio.Semaphore(max)

    async def throttle(self, coroutine):
        try:
            async with self._semaphore:
                return await coroutine
        finally:
            coroutine.close()


async def run_parallel(coroutines):
    tasks = tuple(asyncio.create_task(coroutine) for coroutine in coroutines)
    try:
        for task in tasks:
            await task
    except:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except:
                pass
        raise
