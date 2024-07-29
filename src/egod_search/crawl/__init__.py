from functools import partial
from aiohttp import ClientResponse, ClientSession
from asyncio import QueueEmpty
from aiohttp_retry import ExponentialRetry, RetryClient
from bs4 import BeautifulSoup, SoupStrainer, Tag
from types import TracebackType
from typing import (
    AbstractSet,
    Collection,
    MutableSequence,
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

    Result = tuple[ClientResponse, str | None, Sequence[URL]]
    """
    Crawl result type.
    """
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

    class AlreadyQueued(ValueError):
        """
        Exception for enqueueing an already queued URL.
        """

        __slots__ = ()

    class CrawlError(RuntimeError):
        """
        Exception for crawling errors.
        """

        __slots__ = ()

    __slots__ = ("_queue", "_queued", "_session")

    def __init__(self) -> None:
        """
        Create a crawler.
        """
        self._queue: MutableSequence[URL] = []
        self._queued: MutableSet[URL] = set()

        self._session = RetryClient(
            client_session=ClientSession(),
            retry_options=ExponentialRetry(attempts=10),
            raise_for_status=True,
        )

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

    def enqueue(
        self,
        urls: Collection[URL],
        *,
        before: bool = False,
        ignore_queued: bool = False,
    ) -> None:
        """
        Enqueue URLs to be crawled.

        Raises `ValueError` if the URL is invalid. Raises `URLAlreadyQueued` if the URL has already been visited.
        """
        if unsupported := tuple(
            url for url in urls if url.scheme not in self.SUPPORTED_SCHEMES
        ):
            raise ValueError(f"URL(s) with invalid scheme: {unsupported}")
        insert = partial(self._queue.insert, 0) if before else self._queue.append
        if visited := self._queued & (urls_set := frozenset(urls)):
            if not ignore_queued:
                raise self.AlreadyQueued(*visited)
        self._queued |= urls_set
        for url in reversed(tuple(urls)) if before else urls:
            if url not in visited:
                insert(url)

    def reset(self, urls: Collection[URL]) -> None:
        """
        Mark URLs as unqueued.
        """
        self._queued -= frozenset(urls)

    def dequeue(self) -> URL:
        """
        Dequeue a queued URL for crawling.

        Raises `QueueEmpty` if there are no queued URLs.
        """
        try:
            return self._queue.pop(0)
        except IndexError:
            raise QueueEmpty("No queued URLs")

    async def crawl(self, url: URL) -> Result:
        """
        Crawl the provided URL, enqueue the discovered URLs, and return the response, content and discovered URLs.

        Raises `CrawlError` if an exception occurs.
        """
        try:
            async with self._session.get(url, allow_redirects=True) as response:
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
                            charset := parse_content_type(
                                str(
                                    charset  # `charset` might be `ContentMetaAttributeValue`, so need to `str` it
                                )
                            )[1].get("charset")
                        )
                    ):
                        break
                    charset = None
            content = content.decode(charset or "utf-8", errors="replace")

            outlinks = list[URL]()
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
                outlinks.extend(
                    filter(
                        lambda href: href.scheme in self.SUPPORTED_SCHEMES,
                        map(response.url.join, map(URL, hrefs)),
                    )
                )

            return response, content, outlinks
        except Exception as exc:
            raise self.CrawlError(url) from exc

    @property
    def queue(self) -> Sequence[URL]:
        """
        URLs to be visited.
        """
        return tuple(self._queue)

    @property
    def queued(self) -> AbstractSet[URL]:
        """
        Already queued URLs.
        """
        return frozenset(self._queued)
