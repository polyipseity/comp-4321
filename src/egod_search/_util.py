# -*- coding: UTF-8 -*-
from datetime import datetime
from aiosqlite import Connection
from typing import Any, Iterator, Protocol, TypeVar

_AnyStr_co = TypeVar("_AnyStr_co", str, bytes, covariant=True)
_AnyStr_contra = TypeVar("_AnyStr_contra", str, bytes, contravariant=True)
_T = TypeVar("_T")

_HTTP_LAST_MODIFIED = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}


class SupportsRead(Protocol[_AnyStr_co]):
    """
    Supports reading.
    """

    __slots__ = ()

    def read(self, /) -> _AnyStr_co:
        """
        Read a string.
        """
        ...


class SupportsWrite(Protocol[_AnyStr_contra]):
    """
    Supports writing.
    """

    __slots__ = ()

    def write(self, s: _AnyStr_contra, /) -> object:
        """
        Write a string.
        """
        ...


async def a_fetch_one(conn: Connection, *args: Any) -> Any:
    return await (await conn.execute(*args)).fetchone()


async def a_fetch_value(conn: Connection, *args: Any, default: Any = None) -> Any:
    ret = await a_fetch_one(conn, *args)
    return default if ret is None else ret[0]


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


def parse_http_last_modified(val: str) -> datetime:
    val = val[5:]
    for m_key, m_val in _HTTP_LAST_MODIFIED.items():
        if m_key in val:
            val = val.replace(m_key, m_val, 1)
            break
    return datetime.strptime(
        val.replace("GMT", "+0000"), "%d %m %Y %H:%M:%S %z"
    )  # `%Z` does not work, see https://bugs.python.org/issue22377`


def str_or_repr(obj: object) -> str:
    """
    Return `obj` as is if it is a string, otherwise `repr(obj)`.
    """
    return obj if isinstance(obj, str) else repr(obj)
