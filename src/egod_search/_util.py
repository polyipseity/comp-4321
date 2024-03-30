# -*- coding: UTF-8 -*-
from asyncio import (
    Future,
    Queue,
    TaskGroup,
    get_event_loop,
    get_running_loop,
    new_event_loop,
    set_event_loop,
)
from concurrent.futures import Future as CFuture, ThreadPoolExecutor
from datetime import datetime
from email.message import Message
from functools import partial
from multiprocessing.pool import Pool
from sqlite3 import Row
from aiosqlite import Connection
from typing import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
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


async def a_fetch_one(conn: Connection, *args: object) -> Row | None:
    """
    Return the first row of query result if exists or `None`.
    """
    return await (await conn.execute(*args)).fetchone()


async def a_fetch_value(
    conn: Connection, *args: object, default: object = None
) -> object:
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
    queue = Queue[Awaitable[_U] | _Sentinel](max_size)

    async def submit():
        def execute_init():
            set_event_loop(new_event_loop())

        def execute(item: _T):
            return get_event_loop().run_until_complete(func(item))

        def done_callback(future: Future[_U], c_future: CFuture[_U]):
            try:
                future.set_result(c_future.result())
            except Exception as exc:
                future.set_exception(exc)

        with ThreadPoolExecutor(concurrency, initializer=execute_init) as executor:
            async for item in iterable:
                future = loop.create_future()
                future2 = executor.submit(execute, item)
                future2.add_done_callback(partial(done_callback, future))
                await queue.put(future)
        await queue.put(_SENTINEL)

    async with TaskGroup() as tg:
        tg.create_task(submit())
        while not isinstance(item := await queue.get(), _Sentinel):
            queue.task_done()
            yield await item


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
    queue = Queue[Awaitable[_U] | _Sentinel](max_size)

    async def submit():
        loop = get_running_loop()
        async for item in iterable:
            future = loop.create_future()
            pool.apply_async(
                func,
                (item,),
                callback=future.set_result,
                error_callback=future.set_exception,
            )
            await queue.put(future)
        await queue.put(_SENTINEL)

    async with TaskGroup() as tg:
        tg.create_task(submit())
        while not isinstance(item := await queue.get(), _Sentinel):
            queue.task_done()
            yield await item


def parse_content_type(val: str) -> tuple[str, Mapping[str, str]]:
    """
    Parse content type into a dictionary.
    """
    # https://stackoverflow.com/a/75727619
    msg = Message()
    msg["content-type"] = val
    params = msg.get_params([("", "")])
    return params[0][0], dict(params[1:])


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
