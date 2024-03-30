# -*- coding: UTF-8 -*-
from asyncio import (
    BoundedSemaphore,
    Queue,
    QueueEmpty,
    create_task,
    gather,
    get_running_loop,
)
from datetime import datetime
from email.message import Message
from functools import partial
from multiprocessing.pool import Pool
from sqlite3 import Row
from types import EllipsisType
from unittest import IsolatedAsyncioTestCase
from aiosqlite import Connection
from typing import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
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


class _AsyncTestCase_FakeContextVars:
    def run(self, func: Callable[..., _T], *args: object, **kwargs: object) -> _T:
        return func(*args, **kwargs)


class AsyncTestCase(IsolatedAsyncioTestCase):
    """
    `IsolatedAsyncioTestCase` that is compatible with `unittest-parallel`.
    """

    __slots__ = ("_runner",)

    # @override
    def __init__(
        self, methodName: str = "runTest", *args: object, **kwargs: object
    ) -> None:
        """
        Initialize `AsyncTestCase`.
        """
        ret = super().__init__(methodName, *args, **kwargs)
        self._asyncioTestContext = _AsyncTestCase_FakeContextVars()
        return ret


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
    Async map that eagerly evaluates. `func` is run in the same thread.

    Exceptions are only propagated when the items with exception are accessed.
    """
    queue = Queue[Awaitable[_U] | EllipsisType](max_size)

    async def submit():
        try:
            concurrency_limiter = BoundedSemaphore(concurrency)

            async def execute(item: _T):
                async with concurrency_limiter:
                    return await func(item)

            async for item in iterable:
                async with concurrency_limiter:
                    await queue.put(create_task(execute(item)))
        finally:
            await queue.put(...)

    submit_task = create_task(submit())
    try:
        async for item in a_iter_queue(queue):
            if item is ...:
                break
            yield await item
    finally:
        # stop and cleanup unconsumed awaitables
        submit_task.cancel()
        await gather(
            submit_task,
            *(awaitable for awaitable in iter_queue(queue) if awaitable is not ...),
            return_exceptions=True,
        )


async def a_iter_queue(queue: Queue[_T]) -> AsyncIterator[_T]:
    """
    Iterate through a `Queue` without needing to call `task_done`.
    """
    while True:
        ret = await queue.get()
        try:
            yield ret
        finally:
            queue.task_done()


async def a_pool_imap(
    pool: Pool,
    func: Callable[[_T], _U],
    iterable: AsyncIterable[_T],
    *,
    max_size: int = 0,
) -> AsyncIterator[_U]:
    """
    `Pool.imap` for async.

    Exceptions are only propagated when the items with exception are accessed.
    """
    queue = Queue[Awaitable[_U] | EllipsisType](max_size)

    async def submit():
        try:
            loop = get_running_loop()
            async for item in iterable:
                future = loop.create_future()
                pool.apply_async(
                    func,
                    (item,),
                    callback=partial(loop.call_soon_threadsafe, future.set_result),
                    error_callback=partial(
                        loop.call_soon_threadsafe, future.set_exception
                    ),
                )
                await queue.put(future)
        finally:
            await queue.put(...)

    submit_task = create_task(submit())
    try:
        async for item in a_iter_queue(queue):
            if item is ...:
                break
            yield await item
    finally:
        # stop and cleanup unconsumed awaitables
        submit_task.cancel()
        await gather(
            submit_task,
            *(awaitable for awaitable in iter_queue(queue) if awaitable is not ...),
            return_exceptions=True,
        )


def iter_queue(queue: Queue[_T]) -> Iterator[_T]:
    """
    Iterate all items available now in `Queue`.
    """
    while True:
        try:
            ret = queue.get_nowait()
        except QueueEmpty:
            break
        try:
            yield ret
        finally:
            queue.task_done()


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
