# -*- coding: UTF-8 -*-
from asyncio import TaskGroup, gather
from collections import defaultdict
from time import time
from aiosqlite import connect
from anyio import Path
from argparse import ZERO_OR_MORE, ArgumentParser, Namespace
from asyncstdlib import islice as aislice
from bs4 import BeautifulSoup
from functools import wraps
from sys import modules
from tqdm.auto import tqdm
from typing import Callable, Collection, MutableMapping, MutableSequence
from yarl import URL

from .. import VERSION
from .._util import parse_http_last_modified
from ..crawl import Crawler
from ..database.scheme import Scheme
from ..index.transform import default_transform
from .print import summary_s


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
            async with TaskGroup() as tg:
                async for response, outbound_urls in aislice(
                    crawl_ok_responses(), page_count
                ):
                    text = tg.create_task(response.text())
                    url = response.url
                    try:
                        mod_time = int(
                            parse_http_last_modified(
                                response.headers.get("Last-Modified", "")
                            ).timestamp()
                        )
                    except ValueError:
                        mod_time = int(time())
                    text = await text
                    try:
                        size = int(response.headers.get("Content-Length", ""))
                    except ValueError:
                        size = len(text.encode())  # number of bytes

                    html = BeautifulSoup(text, "html.parser")
                    title = (
                        ""
                        if html.title is None
                        else str(html.title)[len("<title>") : -len("</title>")]
                    )
                    plaintext = html.get_text("\n")

                    word_occurrences = defaultdict[
                        str,
                        MutableMapping[
                            Scheme.Page.WordOccurrenceType, MutableSequence[int]
                        ],
                    ](lambda: defaultdict(list))
                    for pos, word in default_transform(title):
                        word_occurrences[word][
                            Scheme.Page.WordOccurrenceType.TITLE
                        ].append(pos)
                    for pos, word in default_transform(plaintext):
                        word_occurrences[word][
                            Scheme.Page.WordOccurrenceType.TEXT
                        ].append(pos)

                    await database.index_page(
                        Scheme.Page(
                            url=url,
                            mod_time=mod_time,
                            size=size,
                            text=text,
                            plaintext=plaintext,
                            title=title,
                            links=outbound_urls,
                            word_occurrences=word_occurrences,
                        ),
                    )

                    await database.conn.commit()
                    progress.update()

        if summary_path is not None:
            await summary_path.write_text(
                await summary_s(
                    database, count=summary_count, show_progress=show_progress
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
        default=1,
        help="maximum number of concurrent requests; a value of more than 1 makes the crawling order nondeterministic; default 1",
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
