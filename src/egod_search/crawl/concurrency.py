# -*- coding: UTF-8 -*-
from asyncio import (
    Event,
    Queue,
    QueueEmpty,
    Semaphore,
    Task,
    TaskGroup,
    gather,
    get_running_loop,
    sleep,
)
from types import TracebackType
from typing import AsyncIterator, Awaitable, Type
from yarl import URL

from . import Crawler
from .._util import iter_queue


class ConcurrentCrawler:
    """
    A wrapper over a crawler that operates on it concurrently.
    Runs until there are no more links to be crawled or is manually stopped.

    Can be run many times.
    """

    __slots__ = (
        "_awake",
        "_crawler",
        "_queue",
        "_init_concurrency",
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
        self._init_concurrency = init_concurrency
        self._queue = Queue[
            Awaitable[tuple[URL, Crawler.Result | Crawler.CrawlError] | QueueEmpty]
        ](max_size)
        self._stopping = Semaphore(0)
        self._task_group = TaskGroup()
        self._tasks = set[Task[object]]()

    async def __aenter__(self) -> AsyncIterator[Crawler.Result | Crawler.CrawlError]:
        """
        Start the crawler.

        Ensure the previous run has finished before calling.
        """
        self._stopping = Semaphore(0)
        self._task_group = TaskGroup()
        await self._task_group.__aenter__()
        for _ in range(self._init_concurrency):
            task = self._task_group.create_task(self.run())
            task.add_done_callback(self._tasks.discard)
            self._tasks.add(task)  # keep a strong reference to the task
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
        Start a crawler instance. Can be started multiple times in a run.
        """
        # always BFS, even if there are multiple instances
        loop = get_running_loop()
        self._stopping.release()
        while True:
            await self._queue.put(future := loop.create_future())
            try:
                try:
                    url = self._crawler.dequeue()
                except QueueEmpty as exc:
                    future.set_result(exc)
                    async with self._stopping:
                        await self._awake.wait()
                    continue
                try:
                    future.set_result((url, await self._crawler.crawl(url)))
                except Crawler.CrawlError as exc:
                    future.set_result((url, exc))
                except BaseException:
                    self._crawler.reset((url,))
                    self._crawler.enqueue((url,), before=True)
                    raise
            except BaseException:
                future.cancel()
                raise

    def stop(self) -> None:
        """
        Stop all crawlers (eventually).
        """
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()

    async def pipe(self) -> AsyncIterator[Crawler.Result | Crawler.CrawlError]:
        """
        Pipe the output. Outbound links are added as output are piped from this iterator.

        It must be called once and only once per run.
        """
        # only one instance allowed
        while self._stopping.locked():
            await sleep(0)  # ensure at least one crawler is running

        try:
            while True:
                try:
                    value = self._queue.get_nowait()
                except QueueEmpty:
                    if self._tasks:
                        if self._stopping.locked():
                            self.stop()  # all crawlers are stopping
                        await sleep(0)  # yield to crawlers
                        continue
                    else:
                        break
                try:
                    value = await value
                    if isinstance(value, QueueEmpty):
                        continue
                    try:
                        url, ret = value
                        yield ret
                        if isinstance(ret, Crawler.CrawlError):
                            continue
                        self._crawler.enqueue(new_urls := ret[2], ignore_queued=True)
                    except BaseException:
                        url = value[0]
                        self._crawler.reset((url,))
                        self._crawler.enqueue((url,), before=True)
                        raise
                finally:
                    self._queue.task_done()

                if new_urls:
                    # new URLs available
                    self._awake.set()
                    self._awake.clear()
                    await sleep(0)  # yield to crawlers
        finally:
            # cleanup eagerly crawled URLs
            self.stop()
            reset_urls = tuple(
                item[0]
                for item in await gather(
                    *iter_queue(self._queue), return_exceptions=True
                )
                if isinstance(item, tuple)
            )
            self._crawler.reset(reset_urls)
            self._crawler.enqueue(reset_urls, before=True)
