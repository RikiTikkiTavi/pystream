from typing import Protocol
from functools import total_ordering


class SupportsAdd(Protocol):
    def __add__(self, other: "SupportsAdd") -> "SupportsAdd": ...


class SupportsCompare(Protocol):
    def __eq__(self, other: object) -> bool: ...

    def __lt__(self, other: object) -> bool: ...

    def __gt__(self, other: object) -> bool: ...


class SupportsAddAndCompare(SupportsAdd, SupportsCompare): ...
