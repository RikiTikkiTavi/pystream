from typing import Any, TypeVar, List, Tuple

from pystream.Stream import Stream

T = TypeVar('T')


def list_collector(stream: Stream[T]) -> List[T]:
    return list(stream)


def tuple_collector(stream: Stream[T]) -> Tuple[T]:
    return tuple(stream)
