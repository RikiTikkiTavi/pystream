from functools import reduce, partial
from typing import Callable, List, Iterable, Any, TypeVar, Generic, Tuple, Union

_AT = TypeVar("_AT")
_RT = TypeVar("_RT")
_AT1 = TypeVar("_AT1")
_RT1 = TypeVar("_RT1")


def _operation(x, fun):
    return fun(x)


def _apply_chain_operations(x: _AT, /, op1: Callable[[_AT], _RT], op2: Callable[[_RT], _RT1]) -> _RT1:
    return op2(op1(x))


def _identity(x):
    return x


class _Empty:
    pass


empty = _Empty()


def _filter(x: _AT, /, predicate: Callable[[_AT], bool]) -> Union[_AT, _Empty]:
    return x if predicate(x) else empty


class Pipe(Generic[_RT]):
    __operation: Callable[[Any], _RT]

    def __init__(self, operation: Callable[[Any], _RT] = _identity):
        # noinspection Mypy
        self.__operation = operation

    def map(self, mapper: Callable[[_RT], _RT1]) -> "Pipe[_RT1]":
        # noinspection Mypy
        return Pipe(partial(_apply_chain_operations, op1=self.__operation, op2=mapper))

    # noinspection PyMethodMayBeStatic
    def filter(self, predicate: Callable[[_RT], bool]) -> 'Pipe[Union[_AT, _Empty]]':
        # noinspection Mypy
        return Pipe(partial(
            _apply_chain_operations,
            op1=self.__operation,
            op2=partial(_filter, predicate)
        ))

    def get_operation(self) -> Callable[[Any], _RT]:
        # noinspection Mypy
        return self.__operation
