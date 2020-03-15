from functools import reduce, partial
from typing import Callable, List, Iterable, Any, TypeVar, Generic, Tuple, Union, Type, cast

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
    return _Empty if x == _Empty or not predicate(cast(_AT, x)) else x


def _map(x: Union[_AT, Type[_Empty]], /, mapper: Callable[[_AT], _RT]) -> Union[_RT, Type[_Empty]]:
    return _Empty if x == _Empty else mapper(cast(_AT, x))


class Pipe(Generic[_RT]):
    __operation: Callable[..., _RT]

    def __init__(self, operation: Callable[..., _RT] = _identity):
        self.__operation = operation

    def map(self, mapper: Callable[[_RT], _RT1]) -> "Pipe[_RT1]":
        return Pipe(
            cast(
                Callable[..., _RT1],
                partial(
                    _apply_chain_operations,
                    op1=self.__operation,
                    op2=partial(_map, mapper=mapper)
                )))

    def filter(self, predicate: Callable[[_RT], bool]) -> 'Pipe[Union[_AT, _Empty]]':
        return Pipe(
            cast(
                Callable[..., Union[_AT, _Empty]],
                partial(
                    _apply_chain_operations,
                    op1=self.__operation,
                    op2=partial(_filter, predicate=predicate)
                )))

    def get_operation(self) -> Callable[[Any], _RT]:
        return self.__operation
