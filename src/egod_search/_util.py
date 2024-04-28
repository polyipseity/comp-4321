# -*- coding: UTF-8 -*-
from asyncio import (
    BoundedSemaphore,
    Future,
    InvalidStateError,
    Queue,
    QueueEmpty,
    TaskGroup,
    create_task,
    gather,
    get_running_loop,
)
from contextlib import asynccontextmanager
from datetime import datetime
from email.message import Message
from functools import partial, wraps
from multiprocessing import get_context
from multiprocessing.pool import Pool
from os import name
from types import EllipsisType
from unittest import IsolatedAsyncioTestCase
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Mapping,
    Protocol,
    TypeVar,
)

from tortoise import Tortoise

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

DEFAULT_MULTIPROCESSING_CONTEXT = get_context("spawn" if name == "nt" else "fork")
"""
The default context for multiprocessing.

See the info and warnings on <https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods>.
"""


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


@wraps(Tortoise.init)  # type: ignore
@asynccontextmanager
async def Tortoise_context(*args: Any, **kwargs: Any) -> AsyncIterator[None]:
    await Tortoise.init(*args, **kwargs)  # type: ignore
    try:
        await Tortoise.generate_schemas()
        yield
    finally:
        await Tortoise.close_connections()


async def a_eager_map(
    func: Callable[[_T], Awaitable[_U]],
    iterable: AsyncIterable[_T],
    *,
    concurrency: int = 1,
    max_size: int = 0,
) -> AsyncIterator[_U]:
    """
    Async map that eagerly evaluates. `func` is run in the same thread.

    Exceptions are propagated in an exception group when the items with
    exceptions are accessed.
    The group of exceptions may include exceptions that occur during eager
    submission of tasks.
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
                    task = create_task(execute(item))
                    try:
                        await queue.put(task)
                    except BaseException:
                        task.cancel()
                        raise
        finally:
            await queue.put(...)

    try:
        async with TaskGroup() as tg:
            submit_task = tg.create_task(submit())
            async for item in a_iter_queue(queue):
                if item is ...:
                    break
                yield await item
        submit_task.result()
    except BaseExceptionGroup as exc:  # type: ignore
        # do not wrap `GeneratorExit` in an exception group
        gen_exits = exc.subgroup(GeneratorExit)  # type: ignore
        if gen_exits is not None:
            raise gen_exits.exceptions[0]
        raise
    finally:
        # stop and cleanup unconsumed awaitables
        await gather(
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

    Exceptions are propagated in an exception group when the items with
    exceptions are accessed.
    The group of exceptions may include exceptions that occur during eager
    submission of tasks.
    """
    queue = Queue[Awaitable[_U] | EllipsisType](max_size)

    async def submit():
        try:

            def callback(future: Future[_T], arg: _T):
                try:
                    future.set_result(arg)
                except InvalidStateError:
                    future.cancel()

            def error_callback(future: Future[object], arg: BaseException):
                try:
                    future.set_exception(arg)
                except InvalidStateError:
                    future.cancel()

            loop = get_running_loop()
            async for item in iterable:
                await queue.put(future := loop.create_future())
                try:
                    pool.apply_async(
                        func,
                        (item,),
                        callback=partial(loop.call_soon_threadsafe, callback, future),  # type: ignore
                        error_callback=partial(
                            loop.call_soon_threadsafe, error_callback, future  # type: ignore
                        ),
                    )
                except ValueError:
                    # the pool has stopped, which likely means the caller no longer needs more result, so discard error
                    future.cancel()
                except BaseException:
                    future.cancel()
                    raise
        finally:
            await queue.put(...)

    try:
        async with TaskGroup() as tg:
            submit_task = tg.create_task(submit())
            async for item in a_iter_queue(queue):
                if item is ...:
                    break
                yield await item
        submit_task.result()
    except BaseExceptionGroup as exc:  # type: ignore
        # do not wrap `GeneratorExit` in an exception group
        gen_exits = exc.subgroup(GeneratorExit)  # type: ignore
        if gen_exits is not None:
            raise gen_exits.exceptions[0]
        raise
    finally:
        # stop and cleanup unconsumed awaitables
        await gather(
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
