from ast import Call
from functools import reduce, partial
from typing import (
    Callable,
    Iterator,
    List,
    Iterable,
    Any,
    TypeVar,
    Generic,
    Tuple,
    Union,
    Type,
    cast,
    no_type_check,
)
from typing_extensions import TypeGuard

_AT = TypeVar("_AT")
_RT = TypeVar("_RT")
_AT1 = TypeVar("_AT1")
_RT1 = TypeVar("_RT1")


class _Empty:
    pass

def _is_not_empty(x: Union[_AT, Type[_Empty]]) -> TypeGuard[_AT]:
    return x != _Empty

def _identity(x: _AT) -> _AT:
    return x


def _apply_chain_operations(
    x: _AT, /, op1: Callable[[_AT], _RT], op2: Callable[[_RT], _RT1]
) -> Union[_RT1, Type[_Empty]]:
    return op2(op1(x))


def _filter(
    x: Union[_AT, Type[_Empty]], /, predicate: Callable[[_AT], bool]
) -> Union[_AT, Type[_Empty]]:
    if not _is_not_empty(x):
        return _Empty
    if not predicate(x):
        return _Empty
    return x

def _map(
    x: Union[_AT, Type[_Empty]], /, mapper: Callable[[_AT], _RT]
) -> Union[_RT, Type[_Empty]]:
    return mapper(x) if _is_not_empty(x) else _Empty


def filter_out_empty(iterable: Iterable[Union[_AT, Type[_Empty]]]) -> Iterator[_AT]:
    return filter(_is_not_empty, iterable)


class Pipe(Generic[_RT]):
    __operation: Callable[[Any], _RT]
    __has_identity: bool

    def __init__(self, operation: Callable[[Any], _RT] = _identity):
        self.__operation = operation

    @no_type_check
    def map(self, mapper: Callable[[_RT], _RT1]) -> "Pipe[_RT1]":
        if self.__operation is _identity:
            return Pipe(partial(_map, mapper=mapper))
        return Pipe(
            partial(
                _apply_chain_operations,
                op1=self.__operation,
                op2=partial(_map, mapper=mapper),
            )
        )

    @no_type_check
    def filter(self, predicate: Callable[[_RT], bool]) -> "Pipe[Union[_AT, _Empty]]":
        if self.__operation is _identity:
            return Pipe(partial(_filter, predicate=predicate))
        return Pipe(
            partial(
                _apply_chain_operations,
                op1=self.__operation,
                op2=partial(_filter, predicate=predicate),
            )
        )

    def get_operation(self) -> Callable[[Any], _RT]:
        return self.__operation
