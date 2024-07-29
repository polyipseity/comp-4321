from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from time import time
from typing import Collection, Mapping, MutableSequence, NamedTuple, Sequence
from bs4 import BeautifulSoup, Tag
from numpy import amax, array, float64, int64, zeros_like
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

    class WordOccurrences(NamedTuple):
        """
        Word occurrences metadata.
        """

        positions: Sequence[int]
        """
        Word positions.
        """
        frequency: int
        """
        Number of occurrences.
        """
        tf_normalized: float
        """
        Term frequency, normalized.

        Calculated by (number of occurrences in the page / max number of occurrences of a word in the page).
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
    word_occurrences: Mapping[str, WordOccurrences]
    """
    Word occurrences in the plaintext.

    Must not contain empty word occurrences.
    """
    word_occurrences_title: Mapping[str, WordOccurrences]
    """
    Word occurrences in the title.
    
    Must not contain empty word occurrences.
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

    word_freqs = array([len(pos) for pos in word_occurrences.values()], dtype=int64)
    word_freqs_title = array(
        [len(pos) for pos in word_occurrences_title.values()], dtype=int64
    )
    if (word_freq_max := amax(word_freqs, initial=0)) > 0:
        word_tfs = word_freqs / word_freq_max
    else:
        assert word_freqs.size == 0
        word_tfs = zeros_like(word_freqs, dtype=float64)
    if (word_freq_max_title := amax(word_freqs_title, initial=0)) > 0:
        word_tfs_title = word_freqs_title / word_freq_max_title
    else:
        assert word_freqs_title.size == 0
        word_tfs_title = zeros_like(word_freqs_title, dtype=float64)

    return IndexedPage(
        url=url,
        mod_time=datetime.fromtimestamp(mod_time, timezone.utc),
        size=size,
        text=page.content,
        plaintext=plaintext,
        title=title,
        links=page.links,
        word_occurrences={
            key: IndexedPage.WordOccurrences(val, frequency=freq, tf_normalized=tf)
            for (key, val), freq, tf in zip(
                word_occurrences.items(), word_freqs, word_tfs, strict=True
            )
        },
        word_occurrences_title={
            key: IndexedPage.WordOccurrences(val, frequency=freq, tf_normalized=tf)
            for (key, val), freq, tf in zip(
                word_occurrences_title.items(),
                word_freqs_title,
                word_tfs_title,
                strict=True,
            )
        },
    )
