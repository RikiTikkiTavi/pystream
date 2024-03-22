from typing import Any, NoReturn, TypeVar, Generic, Optional, Callable, Union, cast

_T = TypeVar("_T")
_S = TypeVar("_S")


class EmptyNullableException(Exception):
    pass


class Nullable(Generic[_T]):
    """Nullable class which wraps Optional types.

    Args:
        nullable (Optional) : Item which can be None.
    """

    def __init__(self, nullable: Optional[_T]):
        self._item = nullable

    def is_present(self) -> bool:
        """Returns True if item is not None."""
        return self._item is not None

    def get(self) -> Union[Optional[_T], NoReturn]:
        """Gets the item if present.

        Raises:
            EmptyNullableException : Attempting to get a missing item.
        """
        if self.is_present():
            return self._item
        raise EmptyNullableException()

    def or_else(self, default_value: _T) -> _T:
        """Returns the item if present, else return the supplied default value.

        Args:
            default_value : Value to return instead of a None value.
        """
        return cast(_T, self._item) if self.is_present() else default_value

    def or_else_throw(self, exception: Union[Exception, Callable[[], Exception]]) -> _T:
        """Returns if present, raises exception if missing.

        Args:
            exception : Either an exception, or a callable which returns an exception.
        """
        if self.is_present():
            return cast(_T, self._item)
        if isinstance(exception, Exception):
            raise exception
        raise exception()

    def or_else_fetch(self, supplier: Callable[[], _T]) -> _T:
        """Returns if present, invoke callable if missing.

        Args:
            supplier (Callable) : Supplied return value will be return in place of a None value. Should not require parameters.
        """
        if self.is_present():
            return cast(_T, self._item)
        return supplier()

    def if_present(self, consumer: Callable[[_T], None]) -> "Nullable[_T]":
        """Invoke function if value is present; otherwise does nothing.

        Args:
            consumer (Callable) : Function to be invoked with a non-nil parameter.
        """
        if self.is_present():
            consumer(cast(_T, self._item))
        return self

    def filter(
        self, predicate: Union[Callable[[_T], bool], Callable[..., bool]]
    ) -> "Nullable[_T]":
        """Filters item given a criterion.

        Args:
            predicate (Callable) : Invoked with a non-nil parameter. Should return a boolean.

        """
        if self.is_present():
            return self if predicate(cast(_T, self._item)) else Nullable.empty()
        return Nullable.empty()

    def map(self, callable: Callable[[_T], _S]) -> "Nullable[_S]":
        """Maps the item when present.

        Args:
            callable (Callable) : Invoked with a non-nil parameter.
        """
        if self.is_present():
            return Nullable(callable(cast(_T, self._item)))
        return Nullable.empty()

    def __bool__(self) -> bool:
        return self.is_present()

    @staticmethod
    def empty() -> "Nullable[Any]":
        return Nullable(None)
