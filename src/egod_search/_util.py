# -*- coding: UTF-8 -*-
from asyncio import Queue, gather, get_running_loop
from dataclasses import dataclass
from datetime import datetime
from email.message import Message
from multiprocessing.pool import Pool
from aiosqlite import Connection
from asyncstdlib import batched as abatched
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    Mapping,
    Protocol,
    TypeVar,
)

_AnyStr_co = TypeVar("_AnyStr_co", str, bytes, covariant=True)
_AnyStr_contra = TypeVar("_AnyStr_contra", str, bytes, contravariant=True)
_T = TypeVar("_T")
_U = TypeVar("_U")

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


class _Sentinel:
    __slots__ = ()


_SENTINEL = _Sentinel()


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


@dataclass(slots=True)
class Value(Generic[_T]):
    """
    A value container.
    """

    val: _T
    """
    The contained value.
    """


async def a_fetch_one(conn: Connection, *args: Any) -> Any:
    """
    Return the first row of query result if exists or `None`.
    """
    return await (await conn.execute(*args)).fetchone()


async def a_fetch_value(conn: Connection, *args: Any, default: Any = None) -> Any:
    """
    Return the first value of first row of query result if exists or `None`.
    """
    ret = await a_fetch_one(conn, *args)
    return default if ret is None else ret[0]


async def a_eager_map(
    func: Callable[[_T], Awaitable[_U]],
    iterable: AsyncIterable[_T],
    *,
    concurrency: int = 1,
    max_size: int = 0,
) -> AsyncIterator[_U]:
    """
    Async map that eagerly evaluates.
    """
    loop = get_running_loop()
    queue = Queue[_U | _Sentinel](max_size)

    async def submit():
        async for items in abatched(iterable, concurrency):
            for item in await gather(*map(func, items)):
                await queue.put(item)
        await queue.put(_SENTINEL)

    loop.create_task(submit())
    while not isinstance(item := await queue.get(), _Sentinel):
        queue.task_done()
        yield item


async def a_pool_imap(
    pool: Pool,
    func: Callable[[_T], _U],
    iterable: AsyncIterable[_T],
    *,
    max_size: int = 0,
) -> AsyncIterator[_U]:
    """
    `Pool.imap` for async.
    """
    loop = get_running_loop()
    queue = Queue[Awaitable[_U] | _Sentinel](max_size)

    async def submit():
        async for item in iterable:
            fut = loop.create_future()
            await queue.put(fut)
            pool.apply_async(
                func,
                (item,),
                callback=fut.set_result,
                error_callback=fut.set_exception,
            )
        await queue.put(_SENTINEL)

    loop.create_task(submit())
    while not isinstance(item := await queue.get(), _Sentinel):
        queue.task_done()
        yield await item


def parse_content_type(val: str) -> tuple[str, Mapping[str, str]] | None:
    """
    Parse content type into a dictionary.
    """
    # https://stackoverflow.com/a/75727619
    msg = Message()
    msg["content-type"] = val
    params = msg.get_params()
    return None if params is None else (params[0][0], dict(params[1:]))


def parse_http_datetime(val: str) -> datetime:
    """
    Parse datetime format in HTTP headers.
    """
    val = val[5:]
    for m_key, m_val in _HTTP_LAST_MODIFIED.items():
        if m_key in val:
            val = val.replace(m_key, m_val, 1)
            break
    return datetime.strptime(
        val.replace("GMT", "+0000"), "%d %m %Y %H:%M:%S %z"
    )  # `%Z` does not work, see https://bugs.python.org/issue22377`
