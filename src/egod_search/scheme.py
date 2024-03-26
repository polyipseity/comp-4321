from collections import defaultdict
from dataclasses import dataclass
from aiosqlite import Connection
from datetime import datetime, timezone
from importlib.resources import files
from io import StringIO
from itertools import chain, islice
from json import dumps, loads
from re import compile
from types import TracebackType
from typing import AbstractSet, Any, MutableSequence, NewType, Self, Sequence, Type
from tqdm.auto import tqdm
from yarl import URL

from ._util import SupportsWrite, a_fetch_value

URLID = NewType("URLID", int)
"""
ID for URLs.
"""
WordID = NewType("WordID", int)
"""
ID for words.
"""

_WORD_REGEX = compile(r"[a-zA-Z0-9\-_]+")


class Scheme:
    """
    Database scheme.
    """

    __slots__ = ("_conn", "_own_conn")

    CREATE_TABLES_SCRIPT = (
        files(__package__ or "") / "scheme" / "create_tables.sql"
    ).read_text()
    """
    Script to create tables.
    """

    def __init__(self, conn: Connection, *, own_conn: bool = True) -> None:
        """
        Apply this scheme to a database connection.
        """
        self._own_conn = own_conn
        self._conn = conn

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
        await self._conn.executescript(self.CREATE_TABLES_SCRIPT)
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
        vals = tuple(map(str, contents))
        await self._conn.execute(
            f"""
INSERT OR IGNORE INTO main.urls(content) VALUES {', '.join(('(?)',) * len(vals))}""",
            vals,
        )
        return tuple(
            row[0]
            for row in await self._conn.execute_fetchall(
                f"""
SELECT rowid FROM main.urls WHERE content IN ({', '.join('?' * len(vals))})
""",
                vals,
            )
        )

    async def url_id(self, content: URL, /, *args: Any, **kwargs: Any) -> URLID:
        """
        Same as `url_ids` but for one item. Same options are supported.
        """
        return (await self.url_ids((content,), *args, **kwargs))[0]

    async def word_ids(self, contents: Sequence[str], /) -> Sequence[WordID]:
        """
        Get IDs for words. Assigns new IDs if not already assigned.
        """
        vals = contents
        await self._conn.execute(
            f"""
INSERT OR IGNORE INTO main.words(content) VALUES {', '.join(('(?)',) * len(vals))}""",
            vals,
        )
        return tuple(
            row[0]
            for row in await self._conn.execute_fetchall(
                f"""
SELECT rowid FROM main.words WHERE content IN ({', '.join('?' * len(vals))})
""",
                vals,
            )
        )

    async def word_id(self, content: str, /, *args: Any, **kwargs: Any) -> WordID:
        """
        Same as `word_ids` but for one item. Same options are supported.
        """
        return (await self.word_ids((content,), *args, **kwargs))[0]

    @dataclass(frozen=True, kw_only=True, slots=True)
    class Page:
        """
        Database page scheme.
        """

        url: URL
        """
        Page URL.
        """
        title: str
        """
        Page title.
        """
        text: str
        """
        Page content with markups.
        """
        plaintext: str
        """
        Page content in plaintext. That is, without any markups.
        """
        links: AbstractSet[URL]
        """
        Links in the page.
        """
        mod_time: int | None
        """
        Last modification time.
        """

    async def index_page(self, page: Page, /) -> bool:
        """
        Index an page and return whether the page is actually indexed. Raises `ValueError` if `url_id` is invalid.
        """
        url_id = await self.url_id(page.url)
        old_mod_time: int | None = await a_fetch_value(
            self._conn,
            """
SELECT mod_time FROM main.pages WHERE rowid = ?""",
            (url_id,),
        )
        if (
            page.mod_time is not None
            and old_mod_time is not None
            and page.mod_time <= old_mod_time
        ):
            return False

        await self._conn.execute(
            """
INSERT OR REPLACE INTO main.pages(rowid, mod_time, text, plaintext, title, links) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                url_id,
                page.mod_time,
                page.text,
                page.plaintext,
                page.title,
                dumps(list(map(str, page.links))),
            ),
        )

        # clear index
        await self._conn.execute(
            """
DELETE FROM main.word_occurrences WHERE page_id = ?""",
            (url_id,),
        )

        # index words
        word_matches = defaultdict[str, MutableSequence[int]](list)
        for word_match in _WORD_REGEX.finditer(page.plaintext):
            word_matches[word_match[0]].append(word_match.start())
        word_ids = await self.word_ids(tuple(word_matches))
        await self._conn.execute(
            f"""
INSERT INTO main.word_occurrences(page_id, word_id, positions) VALUES {', '.join(('(?, ?, ?)',) * len(word_ids))}""",
            tuple(
                chain.from_iterable(
                    (url_id, word_id, dumps(positions))
                    for positions, word_id in zip(
                        word_matches.values(), word_ids, strict=True
                    )
                )
            ),
        )
        return True

    async def summary(
        self,
        fp: SupportsWrite[str],
        *,
        count: int | None = None,
        keyword_count: int | None = 10,
        link_count: int | None = 10,
        show_progress: bool = False,
    ) -> None:
        """
        Write a summary of the database to `fp`.

        `count` is the number of results to return. `None` means all results.
        `keyword_count` is the number of keywords, most frequent first, per result. `None` means all keywords.
        `link_count` is the number of links, ordered alphabetically, per result. `None` means all links.
        `show_progress` is whether to show a progress bar.
        """
        separator = ""
        total: int | None = await a_fetch_value(
            self._conn,
            """
SELECT count(*) FROM main.pages""",
        )
        total = (
            count if total is None else total if count is None else min(count, total)
        )
        with tqdm(
            total=total,
            disable=not show_progress,
            desc="writing summary",
            unit="pages",
        ) as progress:
            pages_keys = (
                "main.pages.links",
                "main.pages.mod_time",
                "main.pages.plaintext",
                "main.pages.rowid",
                "main.pages.text",
                "main.pages.title",
                "main.urls.content",
            )
            async with self._conn.execute(
                f"""
SELECT {', '.join(pages_keys)}
FROM main.pages INNER JOIN main.urls ON main.urls.rowid = main.pages.rowid
ORDER BY main.pages.rowid
LIMIT ?""",
                (-1 if count is None else count,),
            ) as pages:
                async for page in pages:
                    fp.write(separator)
                    separator = f"{'-' * 100}\n"
                    fp.write(page[pages_keys.index("main.pages.title")] or "(no title)")
                    fp.write("\n")
                    fp.write(page[pages_keys.index("main.urls.content")])
                    fp.write("\n")
                    mod_time = page[pages_keys.index("main.pages.mod_time")]
                    fp.write(
                        "(no last modification time)"
                        if mod_time is None
                        else datetime.fromtimestamp(mod_time, timezone.utc).isoformat()
                    )
                    fp.write(f", {len(page[pages_keys.index('main.pages.text')])}\n")
                    words_keys = (
                        "main.word_occurrences.frequency",
                        "main.word_occurrences.word_id",
                        "main.words.content",
                    )
                    async with self._conn.execute(
                        f"""
SELECT {', '.join(words_keys)}
FROM main.word_occurrences INNER JOIN main.words ON main.words.rowid = main.word_occurrences.word_id
WHERE page_id = ?
ORDER BY main.word_occurrences.frequency DESC, main.words.content ASC
LIMIT ?""",
                        (
                            page[pages_keys.index("main.pages.rowid")],
                            -1 if keyword_count is None else keyword_count,
                        ),
                    ) as words:
                        word_separator = ""
                        async for word in words:
                            fp.write(word_separator)
                            word_separator = "; "
                            fp.write(
                                f"{word[words_keys.index('main.words.content')]} {word[words_keys.index('main.word_occurrences.frequency')]}"
                            )
                    fp.write("\n")
                    for link in islice(
                        sorted(loads(page[pages_keys.index("main.pages.links")])),
                        link_count,
                    ):
                        fp.write(f"{link}\n")
                    progress.update()

    async def summary_s(self, *args: Any, **kwargs: Any) -> str:
        """
        Same as `summary`, except that it returns a string. Same options are supported.
        """
        io = StringIO()
        await self.summary(io, *args, **kwargs)
        return io.getvalue()
