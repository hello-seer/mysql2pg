import typing

T = typing.TypeVar("T")


async def async_count_elements(
    generator: typing.AsyncIterable[T], output: typing.Annotated[typing.List[int], 1]
) -> typing.AsyncIterable[T]:
    count = 0
    async for item in generator:
        count += 1
        yield item
    output.append(count)
