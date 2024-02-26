# -*- coding: UTF-8 -*-
from asyncio import Lock
from types import EllipsisType, TracebackType
from aiohttp import ClientResponse, ClientSession
import httplib2, json, re
from bs4 import BeautifulSoup, SoupStrainer, Tag
from queue import SimpleQueue
from typing import (
    AbstractSet,
    MutableMapping,
    NewType,
    Dict,
    Sequence,
    Set,
    Type,
    TypedDict,
)
from dateutil.parser import parse as parsedate
from itertools import islice
from os.path import isfile
from urllib.parse import urljoin
from yarl import URL

starting_page = "http://www.cse.ust.hk"
number_of_pages = 50
http_cache_path = ".cache"
database_path = "crawled.json"
result_path = "spider_result.txt"

WordId = NewType("WordId", int)
PageId = NewType("PageId", int)
Word = NewType("Word", str)
Url = NewType("Url", str)


class Page(TypedDict):
    title: str
    url: Url
    links: list[Url]
    last_modified: str
    text: str


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
        self._visited: Set[URL] = set()

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

    async def crawl(self) -> ClientResponse:
        """
        Crawl a queued URL, enqueue the discovered URLs, and return the response.

        Raises `TypeError` if there are no queued URLs.
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
            return response

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
            self._visited.update(redirect.url for redirect in response.history)
            self._queue.update(
                {
                    outbound_url: ...
                    for outbound_url in outbound_urls
                    if outbound_url not in self._visited
                }
            )

        return response

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
        return self._visited


if __name__ == "__main__":
    http = httplib2.Http(http_cache_path)
    pages_to_index = SimpleQueue[Url]()
    pages_to_index.put(starting_page)
    index = []

    if not isfile(database_path):
        with open(database_path, "w") as database_file:
            database_file.write("{}")
    with open(database_path, "r+") as database_file:
        database = json.load(database_file)
        word_id_to_word: list[Word] = database.get("word_id_to_word", [])
        word_to_word_id: Dict[Word, WordId] = database.get("word_to_word_id", {})
        url_to_page_id: Dict[Url, PageId] = database.get("url_to_page_id", {})
        page_id_to_url: list[Url] = database.get("page_id_to_url", [])
        forward_index_frequency: Dict[PageId, Dict[WordId, int]] = database.get(
            "forward_index_frequency", {}
        )
        inverted_index_position: Dict[WordId, Dict[PageId, list[int]]] = database.get(
            "inverted_index_position", {}
        )
        pages: list[Page] = database.get("pages", [])
        try:
            pages_indexed = 0
            while pages_indexed < number_of_pages:
                # Get page ID
                url = pages_to_index.get(block=False)
                page_id = url_to_page_id.get(url, PageId(len(url_to_page_id)))
                if page_id == len(url_to_page_id):
                    url_to_page_id[url] = page_id
                    page_id_to_url.append(url)
                    pages.append(
                        {
                            "url": url,
                            "links": [],
                            "last_modified": "0001-01-01T00:00:00+00:00",
                        }
                    )
                # Open the page
                response, html_text = http.request(url)
                if response.status == 200:
                    last_modified = response.get("last-modified", None)
                    if last_modified is None or parsedate(last_modified) > parsedate(
                        pages[page_id]["last_modified"]
                    ):
                        html = BeautifulSoup(html_text, "html.parser")
                        pages[page_id]["last_modified"] = last_modified
                        pages[page_id]["title"] = html.title.string
                        pages[page_id]["links"].clear()
                        pages[page_id]["text"] = html.text

                        forward_index_this_page = forward_index_frequency.get(
                            page_id, {}
                        )
                        forward_index_frequency[page_id] = forward_index_this_page
                        # Get words for indexing
                        for match in re.finditer(
                            r"[a-zA-Z0-9\-_]+", pages[page_id]["text"]
                        ):
                            position, word = match.start(), Word(match.group())

                            word_id = word_to_word_id.get(
                                word, WordId(len(word_to_word_id))
                            )
                            if word_id == len(word_to_word_id):
                                word_to_word_id[word] = word_id
                                word_id_to_word.append(word)

                            forward_index_this_page[word_id] = (
                                forward_index_this_page.get(word_id, 0) + 1
                            )
                            inverted_index_this_word = inverted_index_position.get(
                                word_id, {}
                            )
                            inverted_index_position[word_id] = inverted_index_this_word
                            inverted_index_this_word_this_page = (
                                inverted_index_this_word.get(page_id, [])
                            )
                            inverted_index_this_word_this_page.append(position)
                            inverted_index_this_word[page_id] = (
                                inverted_index_this_word_this_page
                            )

                        # Append outward links for breadth first search
                        for link in BeautifulSoup(
                            html_text, "html.parser", parse_only=SoupStrainer("a")
                        ):
                            if link.has_attr("href"):
                                href = urljoin(url, link["href"])
                                pages[page_id]["links"].append(href)
                                pages_to_index.put(href)
                    pages_indexed = pages_indexed + 1
        finally:
            database_file.seek(0)
            json.dump(
                {
                    "word_id_to_word": word_id_to_word,
                    "word_to_word_id": word_to_word_id,
                    "url_to_page_id": url_to_page_id,
                    "page_id_to_url": page_id_to_url,
                    "forward_index_frequency": forward_index_frequency,
                    "inverted_index_position": inverted_index_position,
                    "pages": pages,
                },
                database_file,
            )

    with open(database_path, "r") as database_file:
        with open(result_path, "w") as result_file:
            database = json.load(database_file)
            for page_id in range(len(database["pages"])):
                page = database["pages"][page_id]
                if "text" in page:  # Only display pages that got us a 200 response
                    result_file.write(page["title"] if page["title"] else "<No Title>")
                    result_file.write("\n")
                    result_file.write(page["url"])
                    result_file.write("\n")
                    result_file.write(
                        page["last_modified"]
                        if page["last_modified"]
                        else "<No last-modified>"
                    )
                    result_file.write(", ")
                    result_file.write(
                        str(len(page["text"])) if page["text"] else "<No text>"
                    )
                    result_file.write("\n")
                    for word_id, frequency in islice(
                        database["forward_index_frequency"][str(page_id)].items(), 10
                    ):
                        result_file.write(database["word_id_to_word"][int(word_id)])
                        result_file.write(" ")
                        result_file.write(str(frequency))
                        result_file.write("; ")
                    result_file.write("\n")
                    for link in islice(page["links"], 10):
                        result_file.write(link)
                        result_file.write("\n")
                    result_file.write("\n")
