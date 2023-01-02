import sys
import typing
from multiprocessing import util


def initializer(initializer, deinitializer):
    def fn(*args):
        initializer(*args)
        util.Finalize(deinitializer, deinitializer, args)

    return fn


T = typing.TypeVar("T")


class Context(typing.Generic[T]):
    value: T

    def _init(self, context: typing.ContextManager[T]):
        self.value = context.__enter__()

    def _deinit(self, context: typing.ContextManager[T]):
        del self.value
        context.__exit__(*sys.exc_info())

    def initializer(self, context: typing.ContextManager[T]):
        return initializer(lambda: self._init(context), lambda: self._deinit(context))
