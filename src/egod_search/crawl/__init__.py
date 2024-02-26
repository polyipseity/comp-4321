# -*- coding: UTF-8 -*-
from asyncio import Lock
from types import EllipsisType, TracebackType
from aiohttp import ClientResponse, ClientSession
from bs4 import BeautifulSoup, SoupStrainer, Tag
from typing import AbstractSet, Collection, MutableMapping, MutableSet, Sequence, Type
from yarl import URL


class Crawler:
    """
    Crawler that supports HTTP and HTTPS.
    """

    __slots__ = ("_lock", "_queue", "_session", "_visited")

    class URLAlreadyVisited(Exception):
        """
        Exception for enqueueing an already visited URL.
        """

        pass

    def __init__(self) -> None:
        self._lock = Lock()
        self._queue: MutableMapping[URL, EllipsisType] = {}
        self._visited: MutableSet[URL] = set()

        self._session = ClientSession()

    async def __aenter__(self) -> "Crawler":
        await self._session.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
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
        if url.scheme not in {"http", "https"}:
            raise ValueError(f"Invalid URL scheme: {url}")
        async with self._lock:
            if url in self._visited:
                raise Crawler.URLAlreadyVisited(f"URL already visited: {url}")
            self._queue[url] = ...

    async def crawl(self) -> tuple[ClientResponse, Collection[URL]]:
        """
        Crawl a queued URL, enqueue the discovered urls, and return the response and discovered urls.

        Raises `TypeError` if there are no queued urls.
        """
        async with self._lock:
            try:
                href_url = next(iter(self._queue))
            except StopIteration:
                raise TypeError()
            self._visited.add(href_url)
            del self._queue[href_url]

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
            self._visited |= frozenset(redirect.url for redirect in response.history)
            self._queue.update(
                {
                    outbound_url: ...
                    for outbound_url in outbound_urls
                    if outbound_url not in self._visited
                }
            )

        return response, outbound_urls

    @property
    def queue(self) -> Sequence[URL]:
        """
        urls to be visited.
        """
        return tuple(self._queue)

    @property
    def visited(self) -> AbstractSet[URL]:
        """
        Already visited urls.
        """
        return self._visited
