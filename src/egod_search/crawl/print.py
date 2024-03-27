# -*- coding: UTF-8 -*-
from datetime import datetime, timezone
from io import StringIO
from json import loads
from typing import Any
from tqdm.auto import tqdm

from .._util import SupportsWrite, a_fetch_value
from ..database.scheme import Scheme


async def summary(
    self: Scheme,
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
    total: int | None = await a_fetch_value(
        self.conn,
        """
SELECT count(*) FROM main.pages""",
    )
    total = count if total is None else total if count is None else min(count, total)
    separator = ""
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
        async with self.conn.execute(
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
                text: str = page[pages_keys.index("main.pages.text")]
                fp.write(f", {len(text.encode())}\n")  # number of bytes

                words_keys = (
                    "main.word_occurrences.frequency",
                    "main.word_occurrences.word_id",
                    "main.words.content",
                )
                async with self.conn.execute(
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

                links_keys = ("main.urls.content",)
                links = loads(page[pages_keys.index("main.pages.links")])
                async with self.conn.execute_fetchall(
                    f"""
SELECT {', '.join(links_keys)}
FROM main.urls
WHERE rowid IN ({', '.join('?' * len(links))})
ORDER BY content
LIMIT ?""",
                    (*links, link_count),
                ) as links:
                    fp.write(
                        "\n".join(
                            link[links_keys.index("main.urls.content")]
                            for link in links
                        )
                    )
                fp.write("\n")

                progress.update()


async def summary_s(self: Scheme, *args: Any, **kwargs: Any) -> str:
    """
    Same as `summary`, except that it returns a string. Same options are supported.
    """
    io = StringIO()
    await summary(self, io, *args, **kwargs)
    return io.getvalue()
