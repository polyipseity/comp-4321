from typing import Iterator, TypeVar

_T = TypeVar("_T")


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
