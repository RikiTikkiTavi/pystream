from functools import partial, wraps
from itertools import islice, chain
from multiprocessing.pool import Pool
from typing import Generator, TypeVar, Tuple, Iterator, Iterable, List, Generic, Callable

T = TypeVar("T")


def partition_generator(iterable: Iterable[T], partition_length: int) -> Generator[Tuple[T, ...], None, None]:
    it: Iterator[T] = iter(iterable)
    while True:
        partition: Tuple[T, ...] = tuple(islice(it, partition_length))
        if len(partition) > 0:
            yield partition
        else:
            break


def reduction_pairs_generator(iterable: Iterable[T]) -> Generator[Tuple[T, ...], None, None]:
    it = iter(iterable)
    while True:
        pair: Tuple[T, ...] = tuple(islice(it, 2))
        if len(pair) == 0:
            break
        yield pair


def fold(
        iterable: Iterable[T],
        /,
        reducer: Callable[[T, T], T],
        pool: Pool,
        chunk_size: int = 1
) -> T:
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
