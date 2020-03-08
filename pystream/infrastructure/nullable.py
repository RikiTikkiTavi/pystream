from typing import TypeVar, Generic, Optional, Callable, Union
import inspect

from pystream.infrastructure.exceptions import EmptyNullableException

T = TypeVar('T')
S = TypeVar('S')


class Nullable(Generic[T]):
    """Nullable class which wraps Optional types.

    Args:
        nullable (Optional) : Item which can be None.
    """

    def __init__(self, nullable: Optional[T]):
        self._item = nullable

    def is_present(self) -> bool:
        """Returns True if item is not None."""
        return self._item is not None

    def get(self) -> Optional[T]:
        """Gets the item if present.

        Raises:
            EmptyNullableException : Attempting to get a missing item.
            """
        if self.is_present():
            return self._item
        raise EmptyNullableException()

    def or_else(self, default_value: T) -> T:
        """Returns the item if present, else return the supplied default value.

        Args:
            default_value : Value to return instead of a None value.
            """
        return self._item if self.is_present() else default_value

    def or_else_throw(self, exception: Union[Exception, Callable[[], Exception]]) -> T:
        """Returns if present, raises exception if missing.

        Args:
            exception : Either an exception, or a callable which returns an exception.
        """
        if self.is_present():
            return self._item
        if isinstance(exception, Exception):
            raise exception
        raise exception()

    def or_else_fetch(self, supplier: Callable[[], T]) -> T:
        """Returns if present, invoke callable if missing.

        Args:
            supplier (Callable) : Supplied return value will be return in place of a None value. Should not require parameters.
        """
        if self.is_present():
            return self._item
        return supplier()

    def if_present(self, consumer: Union[Callable[[T], None], Callable[..., None]]) -> "Nullable[T]":
        """Invoke function if value is present; otherwise does nothing.

        Args:
            consumer (Callable) : Function to be invoked with a non-nil parameter.
        """
        if self.is_present():
            if self.__should_expand(consumer):
                consumer(*self._item)
            else:
                consumer(self._item)
        return self

    def filter(self, predicate: Union[Callable[[T], bool], Callable[..., bool]]) -> "Nullable[T]":
        """Filters item given a criterion.

        Args:
            predicate (Callable) : Invoked with a non-nil parameter. Should return a boolean.

        """
        if self.is_present():
            if self.__should_expand(predicate):
                return self if predicate(*self._item) else Nullable.empty()
            return self if predicate(self._item) else Nullable.empty()
        return Nullable.empty()

    def map(self, callable: Union[Callable[[T], S], Callable[..., S]]) -> "Nullable[S]":
        """Maps the item when present.

        Args:
            callable (Callable) : Invoked with a non-nil parameter.
        """
        if self.is_present():
            if self.__should_expand(callable):
                return Nullable(callable(*self._item))
            return Nullable(callable(self._item))
        return Nullable.empty()

    def __bool__(self) -> bool:
        return self.is_present()

    def __should_expand(self, fun: Callable) -> bool:
        if inspect.isbuiltin(fun):
            return False
        if inspect.isclass(fun):
            return len(inspect.signature(fun.__init__).parameters) > 2
        sig = inspect.signature(fun)
        return len(sig.parameters) > 1

    @staticmethod
    def empty() -> "Nullable":
        return Nullable(None)
