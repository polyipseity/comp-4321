# -*- coding: UTF-8 -*-
from asyncio import gather
from datetime import datetime
from aiosqlite import connect
from anyio import Path
from argparse import ZERO_OR_MORE, ArgumentParser, Namespace
from asyncstdlib import islice as aislice
from bs4 import BeautifulSoup
from functools import wraps
from sys import modules
from tqdm.auto import tqdm
from typing import Callable, Collection
from yarl import URL

from .. import VERSION
from ..crawl import Crawler
from ..scheme import Scheme


async def main(
    urls: Collection[URL],
    *,
    page_count: int | None,
    database_path: Path,
    summary_path: Path | None,
    summary_count: int | None,
    concurrency: int,
    show_progress: bool,
) -> None:
    """
    Main program.
    """

    if page_count is None:
        page_count = len(urls)

    if page_count < 0:
        raise ValueError(f"Page count must be nonnegative: {page_count}")
    if summary_count is not None and summary_count < 0:
        raise ValueError(f"Summary count must be nonnegative: {summary_count}")
    if concurrency <= 0:
        raise ValueError(f"Concurrency must be positive: {concurrency}")

    try:
        async with await database_path.open("xt"):
            pass
    except FileExistsError:
        pass

    async with (
        Scheme(connect(database_path.__fspath__())) as database,
        Crawler() as crawler,
    ):

        async def crawl_ok_responses():
            await crawler.enqueue_many(urls)
            while True:
                # crawl pages concurrently
                responses = await gather(
                    *(crawler.crawl() for _ in range(concurrency)),
                    return_exceptions=True,
                )
                if all(isinstance(response, TypeError) for response in responses):
                    break
                for response in responses:
                    if not isinstance(response, tuple) or not response[0].ok:
                        continue
                    yield response

        with tqdm(
            total=page_count,
            disable=not show_progress,
            desc="crawling",
            unit="pages",
        ) as progress:
            async for response, outbound_urls in aislice(
                crawl_ok_responses(), page_count
            ):
                url = response.url
                try:
                    mod_time = int(
                        datetime.strptime(
                            response.headers.get("Last-Modified", "")[5:],
                            "%d %m %Y %H:%M:%S %Z",
                        ).timestamp()
                    )
                except ValueError:
                    mod_time = None
                text = await response.text()
                html = BeautifulSoup(text, "html.parser")
                await database.index_page(
                    Scheme.Page(
                        url=url,
                        title="" if html.title is None else html.title.string or "",
                        text=text,
                        plaintext=html.text,
                        links=frozenset(outbound_urls),
                        mod_time=mod_time,
                    ),
                    child=True,
                )
                progress.update()
        await database.conn.commit()
        if summary_path is not None:
            await summary_path.write_text(
                await database.summary_s(
                    count=summary_count,
                    show_progress=show_progress,
                    child=True,
                )
            )


def parser(parent: Callable[..., ArgumentParser] | None = None) -> ArgumentParser:
    """
    Create an argument parser suitable for the main program. Pass a parser as `parent` to make this a subparser.
    """

    prog = modules[__name__].__package__ or __name__
    parser = (ArgumentParser if parent is None else parent)(
        prog=f"python -m {prog}",
        description="crawl the internet",
        add_help=True,
        allow_abbrev=False,
        exit_on_error=False,
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"{prog} v{VERSION}",
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
        default=None,
        help="maximum number results in summary; default all",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=10,
        help="maximum number of concurrent requests; default 10",
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
            concurrency=args.concurrency,
            show_progress=args.show_progress,
        )

    parser.set_defaults(invoke=invoke)
    return parser
