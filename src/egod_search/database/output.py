# -*- coding: UTF-8 -*-
from datetime import datetime, timezone
from io import StringIO
from json import loads
from tqdm.auto import tqdm

from .._util import SupportsWrite, a_fetch_value
from .scheme import Scheme


async def summary(
    self: Scheme,
    fp: SupportsWrite[str],
    *,
    count: int = -1,
    keyword_count: int = 10,
    link_count: int = 10,
    show_progress: bool = False,
) -> None:
    """
    Write a summary of the database to `fp`.

    `count` is the maximum number of results to return. Negative means all results.
    `keyword_count` is the maximum number of keywords, most frequent first, per result. Negative values means all keywords.
    `link_count` is the maximum number of links, ordered alphabetically, per result. Negative values means all links.
    `show_progress` is whether to show a progress bar.
    """
    total = await a_fetch_value(
        self.conn,
        """
SELECT count(*) FROM main.pages""",
    )
    assert isinstance(total, int | None)
    total = count if total is None else total if count < 0 else min(count, total)
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
            "main.pages.size",
            "main.pages.title",
            "main.urls.content",
        )
        async with self.conn.execute(
            f"""
SELECT {', '.join(pages_keys)}
FROM main.pages INNER JOIN main.urls USING (rowid)
ORDER BY rowid
LIMIT ?""",
            (count,),
        ) as pages:
            async for page in pages:
                fp.write(separator)
                separator = f"{'-' * 30}\n"  # 100

                fp.write(page[pages_keys.index("main.pages.title")] or "(no title)")
                fp.write("\n")

                fp.write(page[pages_keys.index("main.urls.content")])
                fp.write("\n")

                mod_time = page[pages_keys.index("main.pages.mod_time")]
                fp.write(datetime.fromtimestamp(mod_time, timezone.utc).isoformat())
                fp.write(", ")
                fp.write(
                    str(page[pages_keys.index("main.pages.size")])
                )  # number of bytes
                fp.write("\n")

                words_frequency_key = " + ".join(
                    f"coalesce(word_occurrences{word_type.table_suffix}.frequency, 0)"
                    for word_type in Scheme.Page.WordOccurrenceType
                )
                words_keys = (
                    words_frequency_key,
                    "main.words.content",
                )
                words_outer_joins = " ".join(
                    "FULL OUTER JOIN "
                    f"(SELECT word_id, frequency FROM main.word_occurrences{word_type.table_suffix} WHERE page_id = ?1) "
                    f"AS word_occurrences{word_type.table_suffix} USING (word_id)"
                    for word_type in Scheme.Page.WordOccurrenceType
                    if not word_type.is_default
                )
                async with self.conn.execute(
                    f"""
SELECT {', '.join(words_keys)}
FROM (SELECT word_id, frequency FROM main.word_occurrences WHERE page_id = ?1) AS word_occurrences
    {words_outer_joins}
    INNER JOIN main.words ON main.words.rowid = word_id
ORDER BY {words_frequency_key} DESC, main.words.content ASC
LIMIT ?""",
                    (
                        page[pages_keys.index("main.pages.rowid")],
                        keyword_count,
                    ),
                ) as words:
                    word_separator = ""
                    async for word in words:
                        fp.write(word_separator)
                        word_separator = "; "
                        fp.write(
                            f"{word[words_keys.index('main.words.content')]} {word[words_keys.index(words_frequency_key)]}"
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


async def summary_s(self: Scheme, *args: object, **kwargs: object) -> str:
    """
    Same as `summary`, except that it returns a string. Same options are supported.
    """
    io = StringIO()
    await summary(self, io, *args, **kwargs)
    return io.getvalue()
