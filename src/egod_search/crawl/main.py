# -*- coding: UTF-8 -*-
from asyncio import Event, Lock, Queue, QueueEmpty, Semaphore, TaskGroup, gather, sleep
from collections import defaultdict
from logging import INFO, basicConfig, getLogger
from time import time
from types import EllipsisType
from aiohttp import ClientResponse, ClientResponseError
from aiosqlite import connect
from anyio import Path
from argparse import ZERO_OR_MORE, ArgumentParser, Namespace
from bs4 import BeautifulSoup, Tag
from functools import wraps
from tqdm.auto import tqdm
from typing import Callable, Collection, MutableMapping, MutableSequence, Sequence
from yarl import URL

from .. import VERSION
from .._util import Value, parse_http_datetime
from ..crawl import Crawler
from ..database.scheme import Scheme
from ..index.transform import default_transform
from .output import summary_s

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
        ValueType = tuple[ClientResponse, str | None, Sequence[URL]] | BaseException
        crawl_queue = Queue[Value[tuple[Event, ValueType | None]]]()
        database_queue = Queue[ValueType | EllipsisType]()

        crawling = True
        crawl_dequeue_lock = Lock()
        awake_crawl_event = Event()
        stopping_crawl_semaphore = Semaphore(0)

        async def crawl():
            # always BFS, even if there are multiple instances
            stopping_crawl_semaphore.release()
            while crawling:
                event = Event()
                value = Value[tuple[Event, ValueType | None]]((event, None))
                is_empty = False
                await crawl_queue.put(value)
                try:
                    async with crawl_dequeue_lock:
                        url = await crawler.dequeue()
                    value.val = (event, await crawler.crawl(url))
                except QueueEmpty as exc:
                    # no URLs to crawl
                    value.val = (event, exc)
                    is_empty = True
                except BaseException as exc:
                    value.val = (event, exc)
                finally:
                    event.set()

                if is_empty:
                    async with stopping_crawl_semaphore:
                        # wait for new URLs or stop
                        await awake_crawl_event.wait()

        async def crawl_to_index_bridge(consumer_count: int):
            # only one instance allowed
            while stopping_crawl_semaphore.locked():
                await sleep(0)
            while True:
                try:
                    value = crawl_queue.get_nowait()
                except QueueEmpty:
                    nonlocal crawling
                    if crawling:
                        if stopping_crawl_semaphore.locked():
                            # all crawlers are stopping
                            crawling = False
                            awake_crawl_event.set()
                            awake_crawl_event.clear()
                        await sleep(0)  # yield to crawlers
                        continue
                    else:
                        await gather(
                            *(database_queue.put(...) for _ in range(consumer_count))
                        )  # fill the queue with `...`
                        break
                try:
                    new_urls = None
                    if value.val[1] is None:
                        await value.val[0].wait()
                        assert value.val[1] is not None
                    if isinstance((val1 := value.val[1]), tuple):
                        await crawler.enqueue_many(
                            (new_urls := val1[2]), ignore_visited=True
                        )
                    if not isinstance(val1, QueueEmpty):
                        await database_queue.put(val1)
                finally:
                    crawl_queue.task_done()

                if new_urls:
                    # new URLs available
                    awake_crawl_event.set()
                    awake_crawl_event.clear()
                    await sleep(0)  # yield to crawlers

        pages_crawled = 0
        database_lock = Lock()
        pages_crawled_lock = Lock()

        async def index(progress: Callable[[], object] = lambda: None):
            # multiple instances make the database insertion order nondeterministic
            while (crawled := await database_queue.get()) is not ...:
                try:
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

                    url = response.url
                    try:
                        mod_time = int(
                            parse_http_datetime(
                                response.headers.get(
                                    "Last-Modified",
                                    response.headers.get("Date", ""),
                                )
                            ).timestamp()
                        )
                    except ValueError:
                        mod_time = int(time())

                    html = BeautifulSoup(content, "html.parser")
                    title = (
                        ""
                        if html.title is None
                        else str(html.title)[
                            len("<title>") : -len("</title>")
                        ]  # Google Chrome displays text inside the `title` tag verbatim, including HTML tags. So `<title>a<span>b</span></title>` displays as `a<span>b</span>` instead of `ab`.
                    )
                    for title_tag in html.find_all("title"):
                        assert isinstance(title_tag, Tag)
                        title_tag.extract()
                    plaintext = html.get_text("\n")
                    try:
                        size = int(response.headers.get("Content-Length", ""))
                    except ValueError:
                        size = len(
                            plaintext
                        )  # number of characters in the plaintext, project requirement

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
                            Scheme.Page.WordOccurrenceType.PLAINTEXT
                        ].append(pos)

                    async with database_lock:
                        try:
                            await database.index_page(
                                Scheme.Page(
                                    url=url,
                                    mod_time=mod_time,
                                    size=size,
                                    text=content,
                                    plaintext=plaintext,
                                    title=title,
                                    links=outbound_urls,
                                    word_occurrences=word_occurrences,
                                ),
                            )

                            commit = False
                            nonlocal pages_crawled
                            async with pages_crawled_lock:
                                if pages_crawled < page_count:
                                    pages_crawled += 1
                                    commit = True
                            if commit:
                                await database.conn.commit()
                                progress()
                        finally:
                            await database.conn.rollback()
                finally:
                    database_queue.task_done()
            database_queue.task_done()

        with tqdm(
            total=page_count,
            disable=not show_progress,
            desc="crawling",
            unit="pages",
        ) as progress:
            await crawler.enqueue_many(urls)
            async with TaskGroup() as tg:
                for _ in range(request_concurrency):
                    tg.create_task(crawl())
                tg.create_task(crawl_to_index_bridge(database_concurrency))
                for _ in range(database_concurrency):
                    tg.create_task(index(progress.update))

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
