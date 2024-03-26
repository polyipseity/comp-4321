from dataclasses import dataclass
from aiosqlite import Connection
from datetime import datetime, timezone
from importlib.resources import files
from io import StringIO
from itertools import islice
from json import dumps, loads
from re import compile
from types import TracebackType
from typing import AbstractSet, Any, NewType, Self, Type
from tqdm.auto import tqdm
from yarl import URL

from ._util import SupportsWrite, a_begin, a_fetch_value

"""
Unique identifier based on integers.
"""
URLID = NewType("URLID", int)
"""
ID for URLs.
"""
WordID = NewType("WordID", int)

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

    async def url_id(self, _x: URL, /, child: bool = False) -> URLID:
        """
        Get the URL ID for a url. Assigns a new ID if not already assigned.
        """
        xx = str(_x)
        async with a_begin(self._conn, child) as conn:
            await conn.execute(
                """
INSERT INTO main.urls(content) VALUES(?)
ON CONFLICT(content) DO NOTHING""",
                (xx,),
            )
            return await a_fetch_value(
                conn,
                """
SELECT rowid FROM main.urls WHERE content = ?""",
                (xx,),
            )

    async def word_id(self, _x: str, /, child: bool = False) -> WordID:
        """
        Get the URL ID for a word. Assigns a new ID if not already assigned.
        """
        async with a_begin(self._conn, child) as conn:
            await conn.execute(
                """
INSERT INTO main.words(content) VALUES(?)
ON CONFLICT(content) DO NOTHING""",
                (_x,),
            )
            return await a_fetch_value(
                conn,
                """
SELECT rowid FROM main.words WHERE content = ?""",
                (_x,),
            )

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

    async def index_page(self, _x: Page, /, child: bool = False) -> bool:
        """
        Index an page and return whether the page is actually indexed. Raises `ValueError` if `url_id` is invalid.
        """
        async with a_begin(self._conn, child) as conn:
            url_id = await self.url_id(_x.url, child=True)
            mod_time: int | None = await a_fetch_value(
                conn,
                """
SELECT mod_time FROM main.pages WHERE rowid = ?""",
                (url_id,),
            )
            if (
                _x.mod_time is not None
                and mod_time is not None
                and _x.mod_time <= mod_time
            ):
                return False

            await conn.execute(
                """
INSERT INTO main.pages(rowid, mod_time, text, plaintext, title, links) VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(rowid) DO UPDATE SET
    mod_time = excluded.mod_time,
    text = excluded.text,
    plaintext = excluded.plaintext,
    title = excluded.title,
    links = excluded.links""",
                (
                    url_id,
                    mod_time,
                    _x.text,
                    _x.plaintext,
                    _x.title,
                    dumps(list(map(str, _x.links))),
                ),
            )

            # clear index
            await conn.execute(
                """
DELETE FROM main.word_occurrences WHERE page_id = ?""",
                (url_id,),
            )

            # index words
            for word_match in _WORD_REGEX.finditer(_x.plaintext):
                word, position = word_match[0], word_match.start()
                word_id = await self.word_id(word, child=True)
                await conn.execute(
                    """
INSERT INTO main.word_occurrences(page_id, word_id, positions) VALUES(?, ?, ?)
ON CONFLICT(page_id, word_id) DO UPDATE SET positions = json_insert(positions, '$[#]', json_extract(excluded.positions, '$[0]'))""",
                    (url_id, word_id, dumps([position])),
                )
        return True

    async def summary(
        self,
        fp: SupportsWrite[str],
        *,
        child: bool = False,
        count: int | None = None,
        keyword_count: int | None = 10,
        link_count: int | None = 10,
        show_progress: bool = False,
    ) -> None:
        """
        Write a summary of the database to `fp`.

        `child` is whether this is part of a child transaction.
        `count` is the number of results to return. `None` means all results.
        `keyword_count` is the number of keywords, most frequent first, per result. `None` means all keywords.
        `link_count` is the number of links, ordered alphabetically, per result. `None` means all links.
        `show_progress` is whether to show a progress bar.
        """
        separator = ""
        async with a_begin(self._conn, child) as conn:
            total: int | None = await a_fetch_value(
                conn,
                """
SELECT count(*) FROM main.pages""",
            )
            total = (
                count
                if total is None
                else total if count is None else min(count, total)
            )
            with tqdm(
                total=total,
                disable=not show_progress,
                desc="writing summary",
                unit="pages",
            ) as progress:
                pages_keys = (
                    "links",
                    "mod_time",
                    "plaintext",
                    "rowid",
                    "text",
                    "title",
                )
                async with conn.execute(
                    f"""
SELECT {', '.join(pages_keys)}
FROM main.pages
LIMIT ?""",
                    (-1 if count is None else count,),
                ) as pages:
                    async for page in pages:
                        fp.write(separator)
                        separator = f"{'-' * 100}\n"
                        fp.write(f"{page[pages_keys.index('title')] or '(no title)'}\n")
                        fp.write(
                            await a_fetch_value(
                                conn,
                                """
SELECT content FROM main.urls WHERE rowid = ?""",
                                (page[pages_keys.index("rowid")],),
                            )
                        )
                        fp.write("\n")
                        mod_time = page[pages_keys.index("mod_time")]
                        fp.write(
                            "(no last modification time)"
                            if mod_time is None
                            else f"{datetime.fromtimestamp(mod_time, timezone.utc).isoformat()}, {len(page['plaintext'])}"
                        )
                        fp.write(", ")
                        fp.write(str(len(page[pages_keys.index("text")])))
                        fp.write("\n")
                        words_keys = ("frequency", "word_id")
                        async with conn.execute(
                            f"""
SELECT {', '.join(words_keys)}
FROM main.word_occurrences
WHERE page_id = ?
ORDER BY frequency DESC, word_id ASC
LIMIT ?""",
                            (
                                page[pages_keys.index("rowid")],
                                -1 if keyword_count is None else keyword_count,
                            ),
                        ) as words:
                            word_separator = ""
                            async for word in words:
                                fp.write(word_separator)
                                word_separator = "; "
                                fp.write(
                                    await a_fetch_value(
                                        conn,
                                        """
SELECT content FROM main.words WHERE rowid = ?""",
                                        (word[words_keys.index("word_id")],),
                                    )
                                )
                                fp.write(f" {word[words_keys.index('frequency')]}")
                        fp.write("\n")
                        for link in islice(
                            sorted(loads(page[pages_keys.index("links")])), link_count
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
