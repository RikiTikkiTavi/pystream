from itertools import islice, chain
from multiprocessing.pool import Pool
from typing import Generator, Sequence, TypeVar, Tuple, Iterator, Iterable, List, Generic, Callable, Union, cast

_T = TypeVar("_T")


def partition_generator(iterable: Iterable[_T], partition_length: int) -> Generator[list[_T], None, None]:
    iterator = iter(iterable)
    while True:
        partition: list[_T] = list(islice(iterator, partition_length))
        if len(partition) > 0:
            yield partition
        else:
            break


def reduction_pairs_generator(iterable: Iterable[_T]) -> Generator[Union[tuple[_T, _T], tuple[_T]], None, None]:
    it = iter(iterable)
    while True:
        pair = tuple(islice(it, 2))
        if len(pair) == 0:
            break
        yield cast(Union[tuple[_T, _T], tuple[_T]], pair)


def fold(
        iterable: Iterable[_T],
        /,
        reducer: Callable[[Union[tuple[_T, _T], tuple[_T]]], _T],
        pool: Pool,
        chunk_size: int = 1
) -> _T:
    """
    Parallel fold implementation
    """
    while True:
        iterable = pool.imap(
            func=reducer,
            iterable=reduction_pairs_generator(iterable),
            chunksize=chunk_size
        )
        first_pair = tuple(islice(iterable, 2))
        if len(first_pair) == 1: return first_pair[0]
        iterable = chain(first_pair, iterable)
