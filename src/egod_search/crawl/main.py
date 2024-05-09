# -*- coding: UTF-8 -*-
from asyncio import Lock
from logging import INFO, basicConfig, getLogger
from aiohttp import ClientResponseError
from anyio import Path
from argparse import ZERO_OR_MORE, ArgumentParser, Namespace
from functools import wraps
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from typing import AsyncIterator, Callable, Collection, TypedDict
from yarl import URL

from .. import VERSION
from .._util import (
    DEFAULT_MULTIPROCESSING_CONTEXT,
    Tortoise_context,  # type: ignore
    a_eager_map,
    a_pool_imap,
    tqdmStepper,
)
from ..crawl import Crawler
from ..crawl.concurrency import ConcurrentCrawler
from ..database.output import summary_s
from ..database.models import MODELS, default_config
from ..index import IndexedPage, UnindexedPage, index_page

_PROGRAM = __package__ or __name__
_QUEUE_MAX_SIZE = 1024

_LOGGER = getLogger(_PROGRAM)


async def main(
    urls: Collection[URL],
    *,
    page_count: int,
    database_path: Path,
    summary_path: Path | None,
    summary_count: int,
    keyword_count: int,
    link_count: int,
    request_concurrency: int,
    index_concurrency: int,
    database_concurrency: int,
    show_progress: bool,
) -> None:
    """
    Main program.
    """

    basicConfig(level=INFO)
    with logging_redirect_tqdm():
        if page_count < 0:
            page_count = len(urls)
        if request_concurrency <= 0:
            raise ValueError(
                f"Request concurrency must be positive: {request_concurrency}"
            )
        if index_concurrency <= 0:
            raise ValueError(f"Index concurrency must be positive: {index_concurrency}")
        if database_concurrency <= 0:
            raise ValueError(
                f"Database concurrency must be positive: {database_concurrency}"
            )

        database_path = await database_path.resolve()

        async with (
            Tortoise_context(
                default_config(f"sqlite{database_path.as_uri()[len('file'):]}")
            ),
            tqdmStepper(disable=not show_progress, desc="all", unit="steps") as stepper,
        ):

            async def crawl():
                async with Crawler() as crawler:
                    pages_written = 0
                    database_lock = Lock()

                    async def write(page: IndexedPage | None) -> bool | None:
                        # multiple instances make the database insertion order nondeterministic
                        if page is None:
                            return False
                        async with database_lock:
                            # SQLite does not support concurrency in practice... others may though.
                            nonlocal pages_written
                            if pages_written >= page_count:
                                return False
                            await MODELS.Page.index(MODELS, page)
                            pages_written += 1
                        return True

                    with (
                        DEFAULT_MULTIPROCESSING_CONTEXT.Pool(
                            processes=index_concurrency
                        ) as index_pool,
                        tqdm(
                            total=page_count,
                            disable=not show_progress,
                            desc="crawling",
                            unit="pages",
                        ) as progress,
                    ):
                        crawler.enqueue(urls)
                        async with ConcurrentCrawler(
                            crawler,
                            max_size=_QUEUE_MAX_SIZE,
                            init_concurrency=request_concurrency,
                        ) as responses:

                            async def preprocess() -> AsyncIterator[UnindexedPage]:
                                async for response in responses:
                                    if isinstance(response, Crawler.CrawlError):
                                        _LOGGER.exception(
                                            "Failed to crawl", exc_info=response
                                        )
                                        continue
                                    response, content, outlinks = response
                                    try:
                                        response.raise_for_status()
                                    except ClientResponseError:
                                        _LOGGER.exception("Failed to crawl")
                                        continue
                                    if content is None:
                                        continue
                                    # make the object pickle-able
                                    yield UnindexedPage(
                                        url=response.url,
                                        content=content,
                                        headers=dict(response.headers),
                                        links=outlinks,
                                    )

                            async for written in a_eager_map(
                                write,
                                a_pool_imap(
                                    index_pool,
                                    index_page,
                                    preprocess(),
                                    max_size=_QUEUE_MAX_SIZE,
                                ),
                                concurrency=database_concurrency,
                                max_size=_QUEUE_MAX_SIZE,
                            ):
                                if written:
                                    progress.update()
                                if pages_written >= page_count:
                                    break

            stepper.queue(crawl)

            if summary_path is not None:

                async def summarize():
                    await summary_path.write_text(
                        await summary_s(
                            MODELS,
                            count=summary_count,
                            keyword_count=keyword_count,
                            link_count=link_count,
                            show_progress=show_progress,
                        ),
                        encoding="utf-8",
                    )

                stepper.queue(summarize)

            def finalize():
                _LOGGER.info("ended")

            stepper.queue(finalize)


class ParserOptionDefaults(TypedDict):
    """
    Typing for parser option defaults.
    """

    page_count: int
    summary_path: Path | None
    summary_count: int
    keyword_count: int
    link_count: int
    request_concurrency: int
    index_concurrency: int
    database_concurrency: int
    database_concurrency: int
    show_progress: bool


PARSER_OPTION_DEFAULTS = ParserOptionDefaults(
    page_count=-1,
    summary_path=None,
    summary_count=-1,
    keyword_count=10,
    link_count=10,
    request_concurrency=6,
    index_concurrency=4,
    database_concurrency=1,
    show_progress=True,
)
"""
Default for parser options.
"""


def parser(parent: Callable[..., ArgumentParser] | None = None) -> ArgumentParser:
    """
    Create an argument parser suitable for the main program. Pass a parser as `parent` to make this a subparser.
    """
    parser = (ArgumentParser if parent is None else parent)(
        prog=f"python -m {_PROGRAM}",
        description="crawl the internet",
        add_help=True,
        allow_abbrev=False,
        exit_on_error=False,
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"{_PROGRAM} v{VERSION}",
        help="print version and exit",
    )
    parser.add_argument(
        "inputs",
        action="store",
        nargs=ZERO_OR_MORE,
        type=URL,
        help="initial URL(s) to be crawled",
    )
    parser.add_argument(
        "-n",
        "--page-count",
        type=int,
        default=PARSER_OPTION_DEFAULTS["page_count"],
        help="maximum pages to crawl, negative means the number of inputs; "
        f"default {PARSER_OPTION_DEFAULTS['page_count']}",
    )
    parser.add_argument(
        "-d",
        "--database-path",
        type=Path,
        required=True,
        help="path to database",
    )
    parser.add_argument(
        "-s",
        "--summary-path",
        type=Path,
        default=PARSER_OPTION_DEFAULTS["summary_path"],
        help="path to write database summary; default not write",
    )
    parser.add_argument(
        "--summary-count",
        type=int,
        default=PARSER_OPTION_DEFAULTS["summary_count"],
        help="maximum number results in summary, negative means all; "
        f"default {PARSER_OPTION_DEFAULTS['summary_count']}",
    )
    parser.add_argument(
        "-k",
        "--keyword-count",
        type=int,
        default=PARSER_OPTION_DEFAULTS["keyword_count"],
        help="maximum keywords per result, negative means all; "
        f"default {PARSER_OPTION_DEFAULTS['keyword_count']}",
    )
    parser.add_argument(
        "-l",
        "--link-count",
        type=int,
        default=PARSER_OPTION_DEFAULTS["link_count"],
        help="maximum links per result, negative means all; "
        f"default {PARSER_OPTION_DEFAULTS['link_count']}",
    )
    parser.add_argument(
        "-c",
        "--request-concurrency",
        type=int,
        default=PARSER_OPTION_DEFAULTS["request_concurrency"],
        help="maximum number of concurrent requests, "
        "the crawling order remains deterministic; "
        f"default {PARSER_OPTION_DEFAULTS['request_concurrency']}",
    )
    parser.add_argument(
        "--index-concurrency",
        type=int,
        default=PARSER_OPTION_DEFAULTS["index_concurrency"],
        help="maximum number of concurrent indexing, "
        "the indexing order remains deterministic; "
        f"default {PARSER_OPTION_DEFAULTS['index_concurrency']}",
    )
    parser.add_argument(
        "--database-concurrency",
        type=int,
        default=PARSER_OPTION_DEFAULTS["database_concurrency"],
        help="maximum number of concurrent database write, "
        "a value of more than 1 makes the database order nondeterministic; "
        f"default {PARSER_OPTION_DEFAULTS['database_concurrency']}",
    )
    parser.add_argument(
        "--no-progress",
        action=f"store_{str(not PARSER_OPTION_DEFAULTS["show_progress"]).casefold()}",
        dest="show_progress",
        help="disable showing progress; "
        f"default {str(PARSER_OPTION_DEFAULTS['show_progress']).lower()}",
    )

    @wraps(main)
    async def invoke(args: Namespace):
        await main(
            args.inputs,
            page_count=args.page_count,
            database_path=args.database_path,
            summary_path=args.summary_path,
            summary_count=args.summary_count,
            keyword_count=args.keyword_count,
            link_count=args.link_count,
            request_concurrency=args.request_concurrency,
            index_concurrency=args.index_concurrency,
            database_concurrency=args.database_concurrency,
            show_progress=args.show_progress,
        )

    parser.set_defaults(invoke=invoke)
    return parser
