# -*- coding: UTF-8 -*-
from asyncio import (
    Event,
    Lock,
    Queue,
    QueueEmpty,
    Semaphore,
    Task,
    TaskGroup,
    get_running_loop,
    sleep,
)
from types import TracebackType
from typing import AsyncIterator, Awaitable, Type

from . import Crawler


class ConcurrentCrawler:
    """
    A wrapper over a crawler that operates on it concurrently.

    Runs until there are no more links to be crawled or is manually stopped. Cannot be reused afterwards.
    """

    __slots__ = (
        "_awake",
        "_crawler",
        "_dequeue_lock",
        "_queue",
        "_init_concurrency",
        "_running",
        "_stopping",
        "_task_group",
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
        self._queue = Queue[Awaitable[Crawler.Result | Exception]](max_size)
        self._running = True
        self._stopping = Semaphore(0)
        self._task_group = TaskGroup()
        self._tasks = set[Task[object]]()

    async def __aenter__(self) -> AsyncIterator[Crawler.Result | Exception]:
        """
        Start the crawler.
        """
        await self._task_group.__aenter__()
        for _ in range(self._init_concurrency):
            task = self._task_group.create_task(self.run())
            self._tasks.add(task)  # keep a strong reference to the task
            task.add_done_callback(self._tasks.remove)
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
        await self._task_group.__aexit__(exc_type, exc_val, exc_tb)

    async def run(self) -> None:
        """
        Run the crawler.

        Can be started multiple times. Cannot start again after stopping.
        """
        # always BFS, even if there are multiple instances
        loop = get_running_loop()
        self._stopping.release()
        while self._running:
            await self._queue.put((future := loop.create_future()))
            try:
                async with self._dequeue_lock:
                    url = await self._crawler.dequeue()
                future.set_result(await self._crawler.crawl(url))
            except Exception as exc:
                future.set_result(exc)
                if isinstance(exc, QueueEmpty):
                    # no URLs to crawl
                    async with self._stopping:
                        # wait for new URLs or stop
                        await self._awake.wait()

    def stop(self) -> None:
        """
        Stop all crawlers (eventually). Cannot be restarted again.
        """
        self._running = False
        self._awake.set()

    async def pipe(self) -> AsyncIterator[Crawler.Result | Exception]:
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
                value = await value
                if not isinstance(value, QueueEmpty):
                    yield value
                if isinstance(value, tuple):
                    await self._crawler.enqueue_many(
                        new_urls := value[2], ignore_visited=True
                    )
            finally:
                self._queue.task_done()

            if new_urls:
                # new URLs available
                self._awake.set()
                self._awake.clear()
                await sleep(0)  # yield to crawlers
