from functools import reduce
from typing import Callable, List, Iterable, Any


class Pipe:
    __ops: List[Callable[[Iterable], Iterable]]
    __executed: bool

    def __init__(self):
        self.__ops = []
        self.__executed = False

    def __check_not_executed(self):
        if self.__executed:
            raise TypeError("Pipe already executed.")

    def append(self, op: Callable[[Iterable], Iterable]) -> None:
        self.__check_not_executed()
        self.__ops.append(op)

    def to_iterable(self, iterable: Iterable) -> Iterable:
        self.__check_not_executed()
        self.__executed = True
        return reduce(lambda seq, fun: fun(seq), self.__ops, iterable)
