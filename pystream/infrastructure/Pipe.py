from functools import reduce
from typing import TypeVar, Generic, Callable, List, Iterable


class Pipe:
    __operations: List[Callable]
    __terminated: bool

    def __init__(self):
        self.__operations = []
        self.__terminated = False

    def __check_not_terminated(self):
        if self.__terminated:
            raise TypeError("Pipe already terminal operation.")

    def append_intermediate_operation(self, operation: Callable) -> None:
        self.__check_not_terminated()
        self.__operations.append(operation)

    def append_terminal_operation(self, operation: Callable) -> None:
        self.__check_not_terminated()
        self.__operations.append(operation)
        self.__terminated = True

    def execute(self, iterable: Iterable):
        return reduce(lambda seq, fun: fun(seq), self.__operations, iterable)
