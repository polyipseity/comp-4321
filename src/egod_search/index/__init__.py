# -*- coding: UTF-8 -*-
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from time import time
from typing import Collection, Mapping, MutableMapping, MutableSequence
from bs4 import BeautifulSoup, Tag
from yarl import URL

from .transform import default_transform
from .._util import parse_http_datetime
from ..database.scheme import Scheme


@dataclass(frozen=True, kw_only=True, slots=True)
class UnindexedPage:
    """
    Page metadata for indexing.
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


def index_page(page: UnindexedPage):
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

    word_occurrences = defaultdict[
        str,
        MutableMapping[Scheme.Page.WordOccurrenceType, MutableSequence[int]],
    ](partial(defaultdict, list))
    for pos, word in default_transform(title):
        word_occurrences[word][Scheme.Page.WordOccurrenceType.TITLE].append(pos)
    for pos, word in default_transform(plaintext):
        word_occurrences[word][Scheme.Page.WordOccurrenceType.PLAINTEXT].append(pos)

    return Scheme.Page(
        url=url,
        mod_time=mod_time,
        size=size,
        text=page.content,
        plaintext=plaintext,
        title=title,
        links=page.links,
        word_occurrences=word_occurrences,
    )
