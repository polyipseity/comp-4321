# -*- coding: UTF-8 -*-
from aiohttp import ClientResponse, ClientSession
from asyncio import Lock
from bs4 import BeautifulSoup, SoupStrainer, Tag
from sys import modules
from types import EllipsisType, TracebackType
from typing import (
    AbstractSet,
    Collection,
    MutableMapping,
    MutableSet,
    Self,
    Sequence,
    Type,
)
from yarl import URL


class Crawler:
    """
    Crawler that supports HTTP and HTTPS.
    """

    __slots__ = ("_lock", "_queue", "_session", "_visited")
    SUPPORTED_SCHEMES = frozenset({"http", "https"})
    """
    Supported URL schemes.
    """

    class URLAlreadyVisited(ValueError):
        """
        Exception for enqueueing an already visited URL.
        """

        __slots__ = ()

    class CrawlError(RuntimeError):
        """
        Exception for crawling errors.
        """

        __slots__ = ()

    def __init__(self) -> None:
        """
        Create a crawler.
        """
        self._lock = Lock()
        self._queue: MutableMapping[URL, EllipsisType] = {}
        self._visited: MutableSet[URL] = set()

        self._session = ClientSession()

    async def __aenter__(self) -> Self:
        """
        Use this crawler as a context manager.
        """
        await self._session.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Cleanup the crawler as a context manager.
        """
        await self.aclose()

    async def aclose(self) -> None:
        """
        Cleanup the crawler.
        """
        await self._session.close()

    async def enqueue(self, url: URL) -> None:
        """
        Enqueue a URL to be crawled.

        Raises `ValueError` if the URL is invalid. Raises `URLAlreadyVisited` if the URL has already been visited.
        """
        if url.scheme not in self.SUPPORTED_SCHEMES:
            raise ValueError(f"URL with invalid scheme: {url}")
        async with self._lock:
            if url in self._visited:
                raise self.URLAlreadyVisited(url)
            self._queue[url] = ...

    async def enqueue_many(self, urls: Collection[URL]) -> None:
        """
        Enqueue multiple URLs to be crawled. See `enqueue`.
        """
        if unsupported := tuple(
            url for url in urls if url.scheme not in self.SUPPORTED_SCHEMES
        ):
            raise ValueError(f"URL(s) with invalid scheme: {unsupported}")
        async with self._lock:
            if visited := self._visited & frozenset(urls):
                raise self.URLAlreadyVisited(*visited)
            self._queue.update((url, ...) for url in urls)

    async def crawl(self) -> tuple[ClientResponse, Collection[URL]]:
        """
        Crawl a queued URL, enqueue the discovered URLs, and return the response and discovered URLs.

        Raises `TypeError` if there are no queued URLs. Raises `CrawlError` if we failed to crawl.
        """
        async with self._lock:
            try:
                href_url = next(iter(self._queue))
            except StopIteration:
                raise TypeError("No queued URLs")
            self._visited.add(href_url)
            del self._queue[href_url]

        try:
            async with self._session.get(href_url) as response:
                content = await response.text()
            if not response.ok or response.content_type not in {
                "application/xhtml+xml",
                "application/xml",
                "text/html",
            }:
                return response, ()

            outbound_urls = list[URL]()
            for a_tag in BeautifulSoup(
                content, "html.parser", parse_only=SoupStrainer("a")
            ):
                assert isinstance(a_tag, Tag)
                try:
                    hrefs = a_tag["href"]
                except KeyError:
                    continue
                if isinstance(hrefs, str):
                    hrefs = (hrefs,)
                for href in hrefs:
                    href_url = URL(href)
                    if href_url.is_absolute():
                        outbound_urls.append(href_url)
                    else:
                        outbound_urls.append(response.url.join(href_url))

            async with self._lock:
                self._visited |= frozenset(
                    redirect.url for redirect in response.history
                )
                self._queue.update(
                    (outbound_url, ...)
                    for outbound_url in outbound_urls
                    if outbound_url not in self._visited
                )

            return response, outbound_urls
        except Exception as exc:
            raise self.CrawlError(href_url) from exc

    @property
    def queue(self) -> Sequence[URL]:
        """
        URLs to be visited.
        """
        return tuple(self._queue)

    @property
    def visited(self) -> AbstractSet[URL]:
        """
        Already visited URLs.
        """
        return frozenset(self._visited)


if "unittest" in modules:
    from .test_main import *
    from .test_output import *
