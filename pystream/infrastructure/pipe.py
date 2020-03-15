from functools import reduce, partial
from typing import Callable, List, Iterable, Any, TypeVar, Generic, Tuple, Union, Type

_AT = TypeVar("_AT")
_RT = TypeVar("_RT")
_AT1 = TypeVar("_AT1")
_RT1 = TypeVar("_RT1")


class _Empty:
    pass


def _apply_chain_operations(
        x: _AT,
        /,
        op1: Callable[[_AT], _RT],
        op2: Callable[[_RT], _RT1]
) -> Union[_RT1, Type[_Empty]]:
    return op2(op1(x))


def _identity(x):
    return x


def _filter(x: Union[_AT, Type[_Empty]], /, predicate: Callable[[_AT], bool]) -> Union[_AT, Type[_Empty]]:
    # noinspection Mypy
    return _Empty if x == _Empty or not predicate(x) else x


def _map(x: Union[_AT, Type[_Empty]], /, mapper: Callable[[_AT], _RT]) -> Union[_RT, Type[_Empty]]:
    # noinspection Mypy
    return _Empty if x == _Empty else mapper(x)


class MaybeEmpty(Generic[_AT]):
    __x: _AT
    __empty: bool

    def __init__(self, x: _AT):
        self.__x = x
        self.__empty = False

    def filter(self, predicate: Callable[[_AT], bool]):
        if self.__empty:
            return self
        if predicate(self.__x):
            return self
        self.__empty = True
        del self.__x
        return self

    def map(self, mapper: Callable[[_AT], _RT]):
        if self.__empty:
            return self
        else:
            self.__x = mapper(self.__x)
            return self

    def is_empty(self):
        return self.__empty


def _init_operation(x: _AT):
    return MaybeEmpty(x)


class Pipe(Generic[_RT]):
    __operation: Callable[[Any], _RT]

    def __init__(self, operation: Callable[[Any], _RT] = _identity):
        # noinspection Mypy
        self.__operation = operation

    def map(self, mapper: Callable[[_RT], _RT1]) -> "Pipe[_RT1]":
        # noinspection Mypy
        return Pipe(partial(
            _apply_chain_operations,
            op1=self.__operation,
            op2=partial(_map, mapper=mapper)
        ))

    # noinspection PyMethodMayBeStatic
    def filter(self, predicate: Callable[[_RT], bool]) -> 'Pipe[Union[_AT, _Empty]]':
        # noinspection Mypy
        return Pipe(partial(
            _apply_chain_operations,
            op1=self.__operation,
            op2=partial(_filter, predicate=predicate)
        ))

    def get_operation(self) -> Callable[[Any], _RT]:
        # noinspection Mypy
        return self.__operation
