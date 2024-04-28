# -*- coding: UTF-8 -*-
from asyncio import gather
from collections import defaultdict
from datetime import timezone
from io import StringIO
from itertools import chain
from tqdm.auto import tqdm

from .._util import SupportsWrite
from ..database.models import Models


async def summary(
    models: Models,
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
    total = await models.Page.all().count()
    total = total if count < 0 else min(count, total)
    separator = ""
    with tqdm(
        total=total,
        disable=not show_progress,
        desc="writing summary",
        unit="pages",
    ) as progress:
        tmp = models.Page.all().order_by("id").prefetch_related("url")
        async for page in tmp.limit(count) if count >= 0 else tmp:
            fp.write(separator)
            separator = f"{'-' * 30}\n"  # 100

            fp.write(page.title or "(no title)")
            fp.write("\n")

            fp.write(page.url.content)
            fp.write("\n")

            fp.write(page.mod_time.astimezone(timezone.utc).isoformat())
            fp.write(", ")
            fp.write(str(page.size))  # number of bytes
            fp.write("\n")

            words = defaultdict[str, int](int)
            for word in chain(
                *await gather(
                    models.WordOccurrence.filter(page=page).prefetch_related("word"),
                    models.WordOccurrenceTitle.filter(page=page).prefetch_related(
                        "word"
                    ),
                )
            ):
                words[word.word.content] += word.frequency
            fp.write(
                "; ".join(
                    (
                        f"{item[0]} {item[1]}"
                        for item in sorted(
                            words.items(), key=lambda item: (-item[1], item[0])
                        )[:keyword_count]
                    )
                )
                if keyword_count >= 0
                else ""
            )
            fp.write("\n")

            tmp = page.outlinks.all().order_by("content")
            async for outlink in tmp.limit(link_count) if link_count >= 0 else tmp:
                fp.write(outlink.content)
                fp.write("\n")

            progress.update()


async def summary_s(models: Models, *args: object, **kwargs: object) -> str:
    """
    Same as `summary`, except that it returns a string. Same options are supported.
    """
    io = StringIO()
    await summary(models, io, *args, **kwargs)
    return io.getvalue()
