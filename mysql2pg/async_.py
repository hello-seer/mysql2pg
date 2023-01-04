import asyncio
import typing


async def run_all(tasks: typing.Iterable):
    tasks = tuple(tasks)
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
