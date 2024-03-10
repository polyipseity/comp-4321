from types import TracebackType
from typing import Callable, Iterable, Iterator, Protocol, Self, Type, TypeVar

_AnyStr_co = TypeVar("_AnyStr_co", str, bytes, covariant=True)
_AnyStr_contra = TypeVar("_AnyStr_contra", str, bytes, contravariant=True)
_T = TypeVar("_T")


class SupportsRead(Protocol[_AnyStr_co]):
    def read(self, /) -> _AnyStr_co: ...


class SupportsWrite(Protocol[_AnyStr_contra]):
    def write(self, s: _AnyStr_contra, /) -> object: ...


class Transaction:
    """
    Context manager that supports rollback on exceptions.
    """

    __slots__ = ("_callables", "_parent")

    def __init__(self, parent: Self | None = None) -> None:
        """
        Initialize a transaction.
        """
        self._callables = list[Callable[[], object]]()
        self._parent = parent

    def __enter__(self) -> Self:
        """
        Start a transaction.
        """
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Finish a transaction, rolling back if an exception occurred.
        """
        if exc_val is not None:
            exceptions = list[Exception]()
            while self._callables:
                callable = self._callables.pop()
                try:
                    callable()
                except Exception as exc:
                    exceptions.append(exc)
            if exceptions:
                raise exc_val from ExceptionGroup(
                    "Exception(s) occurred while rolling back.", exceptions
                )
        if self._parent is not None:
            self._parent.push_many(self._callables)

    def push(self, callable: Callable[[], object]) -> None:
        """
        Add a rollbacker.
        """
        self._callables.append(callable)

    def push_many(self, callables: Iterable[Callable[[], object]]) -> None:
        """
        Add many rollbackers.
        """
        self._callables.extend(callables)


def getitem_or_def(obj: object, key: object, default: object = ...) -> object:
    """
    Return `obj[key]` if possible, otherwise `default`.
    """
    try:
        return obj[key]  # type: ignore
    except Exception:
        return default


def int_or_def(obj: object, default: _T = ...) -> int | _T:
    """
    Convert `obj` into an `int` if possible, otherwise `default`.
    """
    try:
        return int(str_or_repr(obj))
    except (TypeError, ValueError):
        return default


def iter_or_def(obj: object, default: Iterator[object] = iter(())) -> Iterator[object]:
    """
    Get the iterator of `obj` if possible, otherwise `default`.
    """
    try:
        return iter(obj)  # type: ignore
    except Exception:
        return default


def str_or_repr(obj: object) -> str:
    """
    Return `obj` as is if it is a string, otherwise `repr(obj)`.
    """
    return obj if isinstance(obj, str) else repr(obj)
