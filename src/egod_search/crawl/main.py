# -*- coding: UTF-8 -*-
from asyncio import gather
from datetime import datetime
from itertools import islice
from anyio import Path
from argparse import ArgumentParser, Namespace
from asyncstdlib import enumerate as aenumerate
from functools import wraps
from sys import modules
from typing import Callable
from bs4 import BeautifulSoup
from yarl import URL

from .. import VERSION
from ..crawl import Crawler
from ..database import Database
from ..scheme import Scheme
from ..types import (
    Timestamp,
    URLStr,
)

_STARTING_PAGE = URL("https://cse.hkust.edu.hk/~kwtleung/COMP4321/testpage.htm")
_DATABASE_PATH = Path("database.json")
_RESULT_PATH = Path("spider_result.txt")
_NUMBER_OF_PAGES = 30


async def main() -> None:
    """
    Main program.
    """

    try:
        async with await _DATABASE_PATH.open("xt"):
            pass
    except FileExistsError:
        pass

    async with await _DATABASE_PATH.open("r+t") as database_file, Crawler() as crawler:
        database = Database(database_file)
        try:
            typed_database = await database.read()
        except Database.InvalidFormat:
            await database.clear()
            typed_database: object = {}
        typed_database = Scheme(Scheme.init(typed_database))

        with typed_database.lock() as typed_database_val:

            async def crawl_ok_responses():
                await crawler.enqueue(_STARTING_PAGE)
                while True:
                    responses = await gather(
                        crawler.crawl(),
                        crawler.crawl(),
                        crawler.crawl(),
                        crawler.crawl(),
                        crawler.crawl(),
                        return_exceptions=True,
                    )  # crawl 5 pages concurrently
                    if all(isinstance(response, TypeError) for response in responses):
                        break
                    for response in responses:
                        if not isinstance(response, tuple) or not response[0].ok:
                            continue
                        yield response

            async for pages_indexed, (response, outbound_urls) in aenumerate(
                crawl_ok_responses()
            ):
                if pages_indexed >= _NUMBER_OF_PAGES:
                    break

                url_str = URLStr(response.url)
                try:
                    mod_time = Timestamp(
                        int(
                            datetime.strptime(
                                response.headers.get("Last-Modified", "")[5:],
                                "%d %m %Y %H:%M:%S %Z",
                            ).timestamp()
                        )
                    )
                except ValueError:
                    mod_time = None
                html = BeautifulSoup(await response.text(), "html.parser")

                typed_database.index_page(
                    typed_database.url_id(url_str),
                    Scheme.Page(
                        {
                            "title": (
                                "" if html.title is None else html.title.string or ""
                            ),
                            "text": html.text,
                            "links": list(map(URLStr, outbound_urls)),
                            "mod_time": mod_time,
                        }
                    ),
                )

            await database.write(typed_database_val)

    async with await _RESULT_PATH.open("wt") as result_file:
        with typed_database.lock() as typed_database_val:
            for url_id, page in typed_database_val["pages"].items():
                await result_file.write(page["title"] or "(no title)")
                await result_file.write("\n")
                await result_file.write(typed_database_val["urls"][url_id])
                await result_file.write("\n")
                await result_file.write(
                    str(page["mod_time"] or "(no modification time)")
                )
                await result_file.write(", ")
                await result_file.write(
                    str(len(page["text"])) if page["text"] else "(no text)"
                )
                await result_file.write("\n")
                for word_id, frequency in islice(
                    typed_database_val["forward_index"][url_id].items(), 10
                ):
                    await result_file.write(typed_database_val["words"][word_id])
                    await result_file.write(" ")
                    await result_file.write(str(frequency))
                    await result_file.write("; ")
                await result_file.write("\n")
                for link in islice(page["links"], 10):
                    await result_file.write(link)
                    await result_file.write("\n")
                await result_file.write("\n")


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

    @wraps(main)
    async def invoke(args: Namespace):
        await main()

    parser.set_defaults(invoke=invoke)
    return parser
