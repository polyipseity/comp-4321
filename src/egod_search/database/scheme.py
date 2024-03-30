# -*- coding: UTF-8 -*-
from enum import StrEnum
from aiosqlite import Connection
from dataclasses import dataclass
from functools import cached_property
from importlib.resources import files
from itertools import chain
from json import dumps
from types import TracebackType
from typing import Collection, Mapping, NewType, Self, Sequence, Type
from yarl import URL

from .. import PACKAGE_NAME
from .._util import a_fetch_value


class Scheme:
    """
    Database scheme.
    """

    URLID = NewType("URLID", int)
    """
    ID for URLs.
    """
    WordID = NewType("WordID", int)
    """
    ID for words.
    """

    @dataclass(frozen=True, kw_only=True, slots=True)
    class Page:
        """
        Database page scheme.
        """

        class WordOccurrenceType(StrEnum):
            """
            Word occurrence type.
            """

            PLAINTEXT = "plaintext"
            TITLE = "title"

            @cached_property
            def is_default(self):
                """
                Whether this is the default word occurrence type.
                """
                return self == self.PLAINTEXT

            @cached_property
            def table_suffix(self):
                """
                Table suffix for this word occurrence type.
                """
                return "" if self.is_default else f"_{self}"

        url: URL
        """
        Page URL.
        """
        title: str
        """
        Page title.
        """
        size: int
        """
        Raw page content size in bytes.
        """
        text: str
        """
        Raw page content.
        """
        plaintext: str
        """
        Page content in plaintext. That is, without any markups.
        """
        links: Collection[URL]
        """
        Links in the page.
        """
        mod_time: int
        """
        Last modification time.
        """
        word_occurrences: Mapping[str, Mapping[WordOccurrenceType, Collection[int]]]
        """
        Mapping from words to their types and positions in the plaintext.

        Positions are a sorted list of unique nonnegative integers.
        """

    _CREATE_DATABASE_SCRIPT = (
        files(PACKAGE_NAME) / "res/create_database.sql"
    ).read_text()
    __slots__ = ("_conn", "_init", "_own_conn")

    def __init__(
        self, conn: Connection, *, own_conn: bool = True, init: bool = False
    ) -> None:
        """
        Apply this scheme to a database connection.
        """
        self._conn = conn
        self._init = init
        self._own_conn = own_conn

    @property
    def conn(self) -> Connection:
        """
        Get the database connection.
        """
        return self._conn

    async def __aenter__(self) -> Self:
        """
        Initialize the database connection with this scheme.
        """
        if self._own_conn:
            await self._conn.__aenter__()
        if self._init:
            await self._conn.executescript(self._CREATE_DATABASE_SCRIPT)
            await self._conn.commit()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Cleanup. Closes the connection unless specified in the constructor.
        """
        if self._own_conn:
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)  # type: ignore

    async def url_ids(self, contents: Sequence[URL], /) -> Sequence[URLID]:
        """
        Get IDs for URLs. Assigns new IDs if not already assigned.
        """
        if not contents:
            return ()
        vals = tuple(map(str, contents))
        await self._conn.executemany(
            """
INSERT OR IGNORE INTO main.urls(content) VALUES (?)""",
            ((val,) for val in vals),
        )
        return tuple(
            row[0]
            for row in await self._conn.execute_fetchall(
                f"""
SELECT rowid FROM main.urls WHERE content IN ({', '.join('?' * len(vals))})
ORDER BY CASE content {' '.join(('WHEN ? THEN ?',) * len(vals))} END""",
                (*vals, *chain.from_iterable(map(reversed, enumerate(vals)))),
            )
        )

    async def url_id(self, content: URL, /, *args: object, **kwargs: object) -> URLID:
        """
        Same as `url_ids` but for one item. Same options are supported.
        """
        return (await self.url_ids((content,), *args, **kwargs))[0]

    async def word_ids(self, contents: Sequence[str], /) -> Sequence[WordID]:
        """
        Get IDs for words. Assigns new IDs if not already assigned.
        """
        if not contents:
            return ()
        vals = contents
        await self._conn.executemany(
            """
INSERT OR IGNORE INTO main.words(content) VALUES (?)""",
            ((val,) for val in vals),
        )
        return tuple(
            row[0]
            for row in await self._conn.execute_fetchall(
                f"""
SELECT rowid FROM main.words WHERE content IN ({', '.join('?' * len(vals))})
ORDER BY CASE content {' '.join(('WHEN ? THEN ?',) * len(vals))} END""",
                (*vals, *chain.from_iterable(map(reversed, enumerate(vals)))),
            )
        )

    async def word_id(self, content: str, /, *args: object, **kwargs: object) -> WordID:
        """
        Same as `word_ids` but for one item. Same options are supported.
        """
        return (await self.word_ids((content,), *args, **kwargs))[0]

    async def index_page(self, page: Page, /) -> bool:
        """
        Index an page and return whether the page is actually indexed. Raises `ValueError` if `url_id` is invalid.
        """
        url_and_links_id = await self.url_ids(
            (page.url, *{link: ... for link in page.links})
        )
        url_id = url_and_links_id[0]
        old_mod_time = await a_fetch_value(
            self._conn,
            """
SELECT mod_time FROM main.pages WHERE rowid = ?""",
            (url_id,),
        )
        assert isinstance(old_mod_time, int | None)
        if old_mod_time is not None and page.mod_time <= old_mod_time:
            return False

        await self._conn.execute(
            """
INSERT OR REPLACE INTO main.pages(rowid, mod_time, text, plaintext, size, title, links) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                url_id,
                page.mod_time,
                page.text,
                page.plaintext,
                page.size,
                page.title,
                dumps(sorted(url_and_links_id[1:])),
            ),
        )

        # clear index
        for word_type in Scheme.Page.WordOccurrenceType:
            await self._conn.execute(
                f"""
DELETE FROM main.word_occurrences{word_type.table_suffix} WHERE page_id = ?""",
                (url_id,),
            )

        # index words
        word_ids = await self.word_ids(tuple(page.word_occurrences))
        for word_type in Scheme.Page.WordOccurrenceType:
            await self._conn.executemany(
                f"""
INSERT INTO main.word_occurrences{word_type.table_suffix}(page_id, word_id, positions) VALUES (?, ?, ?)""",
                chain.from_iterable(
                    (
                        (url_id, word_id, dumps(tuple(typed_pos)))
                        for type, typed_pos in positions.items()
                        if type == word_type
                    )
                    for positions, word_id in zip(
                        page.word_occurrences.values(), word_ids, strict=True
                    )
                ),
            )

        return True
