# -*- coding: UTF-8 -*-
from asyncio import Lock, gather
from logging import INFO, basicConfig, getLogger
from aiohttp import ClientResponse, ClientResponseError
from aiosqlite import connect
from anyio import Path
from argparse import ZERO_OR_MORE, ArgumentParser, Namespace
from functools import wraps
from tqdm.auto import tqdm
from typing import AsyncIterator, Callable, Collection, Sequence
from yarl import URL

from .. import VERSION
from ..crawl import Crawler
from ..crawl.concurrency import ConcurrentCrawler
from ..database.output import summary_s
from ..database.scheme import Scheme
from ..index import PageMetadata, index_page

_PROGRAM = __package__ or __name__


async def main(
    urls: Collection[URL],
    *,
    page_count: int | None,
    database_path: Path,
    summary_path: Path | None,
    summary_count: int,
    keyword_count: int,
    link_count: int,
    request_concurrency: int,
    database_concurrency: int,
    show_progress: bool,
) -> None:
    """
    Main program.
    """

    basicConfig(level=INFO)
    logger = getLogger(_PROGRAM)

    if page_count is None:
        page_count = len(urls)

    if page_count < 0:
        raise ValueError(f"Page count must be nonnegative: {page_count}")
    if request_concurrency <= 0:
        raise ValueError(f"Request concurrency must be positive: {request_concurrency}")
    if database_concurrency <= 0:
        raise ValueError(
            f"Database concurrency must be positive: {database_concurrency}"
        )

    async with (
        Scheme(connect(database_path.__fspath__()), init=True) as database,
        Crawler() as crawler,
    ):
        pages_crawled = 0
        pages_crawled_lock = Lock()
        database_lock = Lock()

        async def index(
            iterator: AsyncIterator[
                tuple[ClientResponse, str | None, Sequence[URL]] | BaseException
            ],
            progress: Callable[[], object] = lambda: None,
        ):
            # multiple instances make the database insertion order nondeterministic
            async for crawled in iterator:
                if isinstance(crawled, BaseException):
                    if isinstance(crawled, Exception):
                        logger.exception("Failed to crawl", exc_info=crawled)
                        continue
                    raise RuntimeError("Failed to crawl") from crawled
                response, content, outbound_urls = crawled
                try:
                    response.raise_for_status()
                except ClientResponseError:
                    logger.exception("Failed to crawl")
                    continue
                if content is None:
                    continue

                page = index_page(
                    PageMetadata(
                        url=response.url,
                        headers=response.headers,
                        links=outbound_urls,
                    ),
                    content,
                )

                nonlocal pages_crawled
                async with pages_crawled_lock:
                    if pages_crawled < page_count:
                        pages_crawled += 1
                    else:
                        break
                async with (
                    database_lock
                ):  # SQLite does not support concurrency in practice... others may though.
                    await database.index_page(page)
                    await database.conn.commit()
                    progress()

        with tqdm(
            total=page_count,
            disable=not show_progress,
            desc="crawling",
            unit="pages",
        ) as progress:
            await crawler.enqueue_many(urls)
            async with ConcurrentCrawler(
                crawler, init_concurrency=request_concurrency
            ) as responses:
                await gather(
                    *(
                        index(responses, progress.update)
                        for _ in range(database_concurrency)
                    )
                )

    if summary_path is not None:
        async with Scheme(connect(database_path.__fspath__())) as database:
            await summary_path.write_text(
                await summary_s(
                    database,
                    count=summary_count,
                    keyword_count=keyword_count,
                    link_count=link_count,
                    show_progress=show_progress,
                ),
                encoding="utf-8",
            )


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
        default=None,
        help="maximum pages to crawl; default number of inputs",
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
        default=None,
        help="path to write database summary; default not write",
    )
    parser.add_argument(
        "--summary-count",
        type=int,
        default=-1,
        help="maximum number results in summary, negative means all; default -1",
    )
    parser.add_argument(
        "-k",
        "--keyword-count",
        type=int,
        default=10,
        help="maximum keywords per result, negative means all; default 10",
    )
    parser.add_argument(
        "-l",
        "--link-count",
        type=int,
        default=10,
        help="maximum links per result, negative means all; default 10",
    )
    parser.add_argument(
        "-c",
        "--request-concurrency",
        type=int,
        default=6,
        help="maximum number of concurrent requests, the crawling order remains deterministic; default 6",
    )
    parser.add_argument(
        "--database-concurrency",
        type=int,
        default=1,
        help="maximum number of concurrent database write, a value of more than 1 makes the database order nondeterministic; default 1",
    )
    parser.add_argument(
        "--no-progress",
        action="store_false",
        dest="show_progress",
        help="disable showing progress; default enable",
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
            database_concurrency=args.database_concurrency,
            show_progress=args.show_progress,
        )

    parser.set_defaults(invoke=invoke)
    return parser
