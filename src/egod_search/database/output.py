# -*- coding: UTF-8 -*-
from datetime import timezone
from io import StringIO
from tortoise.expressions import RawSQL
from tortoise.query_utils import Prefetch
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
    page_separator = ""
    with tqdm(
        total=total,
        disable=not show_progress,
        desc="writing summary",
        unit="pages",
    ) as progress:
        tmp = (
            models.Page.all()
            .order_by("id")
            .prefetch_related(Prefetch("url", models.URL.all().only("id", "content")))
        )
        async for page in tmp.limit(count) if count >= 0 else tmp:
            fp.write(page_separator)
            page_separator = f"{'-' * 30}\n"  # 100

            fp.write(page.title or "(no title)")
            fp.write("\n")

            fp.write(page.url.content)
            fp.write("\n")

            fp.write(page.mod_time.astimezone(timezone.utc).isoformat())
            fp.write(", ")
            fp.write(str(page.size))  # number of bytes
            fp.write("\n")

            word_separator = ""
            tmp = (
                models.PageWord.filter(page=page)
                .annotate(
                    frequency=RawSQL(
                        f"coalesce((SELECT frequency FROM {models.WordPositions._meta.db_table} WHERE key_id = {models.PageWord._meta.db_table}.id), 0)"  # type: ignore
                    )
                    + RawSQL(
                        f"coalesce((SELECT frequency FROM {models.WordPositionsTitle._meta.db_table} WHERE key_id = {models.PageWord._meta.db_table}.id), 0)"  # type: ignore
                    ),
                )
                .order_by("-frequency", "word__content")
                .prefetch_related(
                    Prefetch("word", models.Word.all().only("id", "content"))
                )
            )
            async for word in tmp.limit(keyword_count) if keyword_count >= 0 else tmp:
                frequency = getattr(word, "frequency")
                assert isinstance(frequency, int)
                fp.write(word_separator)
                word_separator = "; "
                fp.write(f"{word.word.content} {frequency}")
            fp.write("\n")

            tmp = page.outlinks.all().order_by("content").only("content")
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
