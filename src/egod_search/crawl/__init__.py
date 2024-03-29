# -*- coding: UTF-8 -*-
from aiohttp import ClientResponse, ClientSession
from asyncio import Lock, QueueEmpty
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

from .._util import parse_content_type


class Crawler:
    """
    Crawler that supports HTTP and HTTPS.
    """

    __slots__ = ("_lock", "_queue", "_session", "_visited")
    SUPPORTED_CONTENT_TYPES = frozenset(
        {
            "application/xhtml+xml",
            "application/xml",
            "text/html",
        }
    )
    """
    Supported content types.
    """
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

    async def enqueue(self, url: URL, *, ignore_visited: bool = False) -> None:
        """
        Enqueue a URL to be crawled.

        Raises `ValueError` if the URL is invalid. Raises `URLAlreadyVisited` if the URL has already been visited.
        """
        if url.scheme not in self.SUPPORTED_SCHEMES:
            raise ValueError(f"URL with invalid scheme: {url}")
        async with self._lock:
            if url in self._visited:
                if ignore_visited:
                    return
                raise self.URLAlreadyVisited(url)
            self._queue[url] = ...

    async def enqueue_many(
        self, urls: Collection[URL], *, ignore_visited: bool = False
    ) -> None:
        """
        Enqueue multiple URLs to be crawled. See `enqueue`.
        """
        if unsupported := tuple(
            url for url in urls if url.scheme not in self.SUPPORTED_SCHEMES
        ):
            raise ValueError(f"URL(s) with invalid scheme: {unsupported}")
        async with self._lock:
            if visited := self._visited & frozenset(urls):
                if not ignore_visited:
                    raise self.URLAlreadyVisited(*visited)
            self._queue.update((url, ...) for url in urls if url not in visited)

    async def dequeue(self) -> URL:
        """
        Dequeue a queued URL for crawling and mark it as visited.

        Raises `QueueEmpty` if there are no queued URLs.
        """
        async with self._lock:
            try:
                url = next(iter(self._queue))
            except StopIteration:
                raise QueueEmpty("No queued URLs")
            self._visited.add(url)
            del self._queue[url]
        return url

    async def crawl(self, url: URL) -> tuple[ClientResponse, str | None, Sequence[URL]]:
        """
        Crawl the provided URL, enqueue the discovered URLs, and return the response, content and discovered URLs.

        Raises `CrawlError` if an exception occurs.
        """
        try:
            async with self._session.get(url) as response:
                content = await response.read()
            if (
                not response.ok
                or response.content_type not in self.SUPPORTED_CONTENT_TYPES
            ):
                return response, None, ()

            # detect charset, see https://www.w3.org/International/questions/qa-html-encoding-declarations
            if not (charset := response.charset):
                header = content[:1024].decode(errors="ignore")
                for meta_tag in BeautifulSoup(
                    header, "html.parser", parse_only=SoupStrainer("meta")
                ):
                    assert isinstance(meta_tag, Tag)
                    if (charset := meta_tag.get("charset")) and (
                        isinstance(charset, str) or (charset := charset[0])
                    ):
                        break
                    if (
                        (
                            (http_equiv := meta_tag.get("http-equiv"))
                            and "Content-Type".casefold()
                            in map(
                                str.casefold,
                                (
                                    (http_equiv,)
                                    if isinstance(http_equiv, str)
                                    else http_equiv
                                ),
                            )
                        )
                        and (charset := meta_tag.get("content"))
                        and (isinstance(charset, str) or (charset := charset[0]))
                        and (
                            charset := parse_content_type(str(charset))
                        )  # `charset` might actually be `ContentMetaAttributeValue` instead
                        and (charset := charset[1].get("charset"))
                    ):
                        break
                    charset = None
            content = content.decode(charset or "utf-8", errors="replace")

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
                outbound_urls.extend(map(response.url.join, map(URL, hrefs)))

            return response, content, outbound_urls
        except Exception as exc:
            raise self.CrawlError(url) from exc

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
