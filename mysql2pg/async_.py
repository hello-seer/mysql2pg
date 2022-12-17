import asyncio


class Throttle:
    def __init__(self, max: int):
        self._semaphore = asyncio.Semaphore(max)

    async def throttle(self, coroutine):
        async with self._semaphore:
            return await coroutine


async def run_parallel(coroutines):
    tasks = tuple(asyncio.create_task(coroutine) for coroutine in coroutines)
    try:
        for task in tasks:
            await task
    except:
        task.cancel()
        try:
            for task in tasks:
                await task
        except:
            pass
        raise
