import typing

T = typing.TypeVar("T")


def identity(x: T) -> T:
    return x
