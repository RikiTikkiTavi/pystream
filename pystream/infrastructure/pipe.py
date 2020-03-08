from functools import reduce
from typing import Callable, List, Iterable, Any


class Pipe:
    __intermediate: List[Callable[[Iterable], Iterable]]
    __terminal: Callable[[Iterable], Any]
    __terminated: bool
    __executed: bool

    def __init__(self):
        self.__intermediate = []
        self.__terminated = False
        self.__executed = False

    def __check_not_executed(self):
        if self.__terminated:
            raise TypeError("Pipe already executed.")

    def __check_not_terminated(self):
        if self.__terminated:
            raise TypeError("Pipe already has terminal operation.")

    def append_intermediate(self, operation: Callable[[Iterable], Iterable]) -> None:
        self.__check_not_terminated()
        self.__check_not_executed()
        self.__intermediate.append(operation)

    def set_terminal(self, operation: Callable[[Iterable], Any]) -> None:
        self.__check_not_terminated()
        self.__check_not_executed()
        self.__terminal = operation

    def execute(self, iterable: Iterable):
        self.__check_not_executed()
        self.__executed = True
        return reduce(lambda seq, fun: fun(seq), self.__intermediate + [self.__terminal], iterable)
