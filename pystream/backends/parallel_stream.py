from functools import partial, reduce, wraps
from itertools import chain
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, List, Iterable, Tuple
import pystream.infrastructure.utils as utils
import pystream.stream as stream
from itertools import islice

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


def _filter_partition(element: _AT, predicate: Callable[[_AT], bool]) -> List[_AT]:
    return [element] if predicate(element) else []


def _reducer(pair: Tuple[_AT, ...], /, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
    return reducer(*pair) if len(pair) == 2 else pair[0]


class ParallelStreamBackend(Generic[_AT]):
    __scheduled: List[Callable]
    __pool: Pool
    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT]):
        self.__iterable = chain(*iterables)
        self.__scheduled = []


    def map_lazy(
            self,
            iterable: Iterable[_AT],
            /,
            mapper: Callable[[_AT], _RT],
            chunk_size: int
    ) -> Iterable[_RT]:
        """
        Lazy map implementation
        """
        return self.__pool.imap(
            mapper,
            iterable,
            chunksize=chunk_size
        )

    def map_active(
            self,
            iterable: Iterable[_AT],
            /,
            mapper: Callable[[_AT], _RT]
    ) -> Iterable[_RT]:
        """
        Active map implementation
        """
        return self.__pool.map(
            mapper,
            iterable
        )

    def filter_lazy(
            self,
            iterable: Iterable[_AT], /,
            predicate: Callable[[_AT], bool],
            chunk_size: int = 1
    ) -> Iterable[_AT]:
        """
        Lazy filter implementation
        """
        return iter(utils.lazy_flat_generator(self.__pool.imap(
            partial(_filter_partition, predicate=predicate),
            iterable,
            chunksize=chunk_size
        )))

    def filter_active(
            self,
            iterable: Iterable[_AT], /,
            predicate: Callable[[_AT], bool]
    ) -> Iterable[_AT]:
        """
        Active filter implementation
        """
        return iter(utils.lazy_flat_generator(self.__pool.map(
            partial(_filter_partition, predicate=predicate),
            iterable
        )))

    def reduce_lazy(
            self,
            iterable: Iterable[_AT],
            /,
            reducer: Callable[[_AT, _AT], _AT],
            chunk_size: int = 1
    ) -> _AT:
        """
        Lazy reduce implementation
        """
        while True:
            iterable = self.__pool.imap_unordered(
                func=partial(_reducer, reducer=reducer),
                iterable=utils.reduction_pairs_generator(iterable)
            )
            first_pair = tuple(islice(iterable, 2))
            if len(first_pair) == 1:
                return first_pair[0]
            iterable = chain(first_pair, iterable)

    def assemble_iterable(self, pool: Pool) -> Iterable:
        return reduce(lambda seq, fun: fun(seq), self.__scheduled, self.__iterable)
