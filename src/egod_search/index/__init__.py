# -*- coding: UTF-8 -*-
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from time import time
from typing import Collection, Mapping, MutableSequence, Sequence
from bs4 import BeautifulSoup, Tag
from yarl import URL

from .transform import default_transform
from .._util import parse_http_datetime


@dataclass(frozen=True, kw_only=True, slots=True)
class UnindexedPage:
    """
    Unindexed page metadata.
    """

    url: URL
    """
    URL of the page.
    """
    content: str
    """
    Raw content of the page.
    """
    headers: Mapping[str, str]
    """
    HTTP response headers.
    """
    links: Collection[URL]
    """
    Collection of outbound links.
    """


@dataclass(frozen=True, kw_only=True, slots=True)
class IndexedPage:
    """
    Indexed Page metadata.
    """

    url: URL
    """
    URL of the page.
    """
    mod_time: datetime
    """
    Last modification of the page.
    """
    text: str
    """
    Raw content of the page, including markup.
    """
    plaintext: str
    """
    Plaintext of the page, excluding markup.
    """
    size: int
    """
    Size of the page.
    """
    title: str
    """
    Title of the page.
    """
    links: Collection[URL]
    """
    Outgoing links from this page.
    """
    word_occurrences: Mapping[str, Sequence[int]]
    """
    Word occurrences in the plaintext.
    """
    word_occurrences_title: Mapping[str, Sequence[int]]
    """
    Word occurrences in the title.
    """


def index_page(page: UnindexedPage) -> IndexedPage:
    """
    Index a page from its metadata, content, and links.
    """
    url = page.url
    try:
        mod_time = int(
            parse_http_datetime(
                page.headers.get(
                    "Last-Modified",
                    page.headers.get("Date", ""),
                )
            ).timestamp()
        )
    except ValueError:
        mod_time = int(time())

    html = BeautifulSoup(page.content, "html.parser")
    title = (
        ""
        if html.title is None
        else str(html.title)[len("<title>") : -len("</title>")]
        # Google Chrome displays text inside the `title` tag verbatim, including HTML tags.
        # So `<title>a<span>b</span></title>` displays as `a<span>b</span>` instead of `ab`.
    )
    for title_tag in html.find_all("title"):
        assert isinstance(title_tag, Tag)
        title_tag.extract()
    plaintext = html.get_text("\n")
    try:
        size = int(page.headers.get("Content-Length", ""))
    except ValueError:
        size = len(
            plaintext
        )  # number of characters in the plaintext, project requirement

    word_occurrences = defaultdict[str, MutableSequence[int]](list)
    word_occurrences_title = defaultdict[str, MutableSequence[int]](list)
    for pos, word in default_transform(plaintext):
        word_occurrences[word].append(pos)
    for pos, word in default_transform(title):
        word_occurrences_title[word].append(pos)

    return IndexedPage(
        url=url,
        mod_time=datetime.fromtimestamp(mod_time, timezone.utc),
        size=size,
        text=page.content,
        plaintext=plaintext,
        title=title,
        links=page.links,
        word_occurrences=word_occurrences,
        word_occurrences_title=word_occurrences_title,
    )
