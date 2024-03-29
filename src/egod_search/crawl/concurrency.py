# -*- coding: UTF-8 -*-
from asyncio import Event, Lock, Queue, QueueEmpty, Semaphore, TaskGroup, sleep
from types import TracebackType
from typing import AsyncIterator, Sequence, Type
from aiohttp import ClientResponse
from yarl import URL

from . import Crawler
from .._util import Value


class ConcurrentCrawler:
    """
    A wrapper over a crawler that operates on it concurrently.

    Runs until there are no more links to be crawled or is manually stopped. Cannot be reused afterwards.
    """

    _ValueType = tuple[ClientResponse, str | None, Sequence[URL]] | BaseException
    __slots__ = (
        "_awake",
        "_crawler",
        "_dequeue_lock",
        "_queue",
        "_init_concurrency",
        "_running",
        "_stopping",
        "_tasks",
    )

    def __init__(
        self, crawler: Crawler, *, max_size: int = 0, init_concurrency: int = 0
    ) -> None:
        """
        Initialize `ConcurrentCrawler`.
        """
        self._awake = Event()
        self._crawler = crawler
        self._dequeue_lock = Lock()
        self._init_concurrency = init_concurrency
        self._queue = Queue[Value[tuple[Event, self._ValueType | None]]](max_size)
        self._running = True
        self._stopping = Semaphore(0)
        self._tasks = TaskGroup()

    async def __aenter__(self) -> AsyncIterator[_ValueType]:
        """
        Start the crawler.
        """
        await self._tasks.__aenter__()
        for _ in range(self._init_concurrency):
            self._tasks.create_task(self.run())
        return self.pipe()

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Stop and cleanup the crawler.
        """
        self.stop()
        await self._tasks.__aexit__(exc_type, exc_val, exc_tb)

    async def run(self) -> None:
        """
        Run the crawler.

        Can be started multiple times. Cannot start again after stopping.
        """
        # always BFS, even if there are multiple instances
        self._stopping.release()
        while self._running:
            event = Event()
            value = Value[tuple[Event, self._ValueType | None]]((event, None))
            is_empty = False
            await self._queue.put(value)
            try:
                async with self._dequeue_lock:
                    url = await self._crawler.dequeue()
                value.val = (event, await self._crawler.crawl(url))
            except QueueEmpty as exc:
                # no URLs to crawl
                value.val = (event, exc)
                is_empty = True
            except BaseException as exc:
                value.val = (event, exc)
            finally:
                event.set()

            if is_empty:
                async with self._stopping:
                    # wait for new URLs or stop
                    await self._awake.wait()

    def stop(self) -> None:
        """
        Stop all crawlers (eventually). Cannot be restarted again.
        """
        self._running = False
        self._awake.set()

    async def pipe(self) -> AsyncIterator[_ValueType]:
        """
        Pipe the output. Outbound links are added as output are piped from this iterator.

        Can only be called once.
        """
        # only one instance allowed
        while self._stopping.locked():
            await sleep(0)
        while True:
            try:
                value = self._queue.get_nowait()
            except QueueEmpty:
                if self._running:
                    if self._stopping.locked():
                        # all crawlers are stopping
                        self.stop()
                    await sleep(0)  # yield to crawlers
                    continue
                else:
                    break
            try:
                new_urls = None
                if value.val[1] is None:
                    await value.val[0].wait()
                    assert value.val[1] is not None
                if not isinstance((val1 := value.val[1]), QueueEmpty):
                    yield val1
                if isinstance(val1, tuple):
                    await self._crawler.enqueue_many(
                        (new_urls := val1[2]), ignore_visited=True
                    )
            finally:
                self._queue.task_done()

            if new_urls:
                # new URLs available
                self._awake.set()
                self._awake.clear()
                await sleep(0)  # yield to crawlers
