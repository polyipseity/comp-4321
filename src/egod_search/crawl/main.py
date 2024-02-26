# -*- coding: UTF-8 -*-
from asyncio import gather
from bisect import bisect_right
from datetime import datetime
from itertools import islice
from anyio import Path
from argparse import ArgumentParser, Namespace
from asyncstdlib import enumerate as aenumerate
from functools import wraps
from re import compile
from sys import modules
from typing import Callable
from bs4 import BeautifulSoup
from yarl import URL

from .. import VERSION
from ..crawl import Crawler
from ..database import Database
from ..scheme import (
    NULL_TIME,
    URLID,
    Scheme,
    Timestamp,
    Word,
    WordFrequency,
    WordID,
    WordPosition,
    gen_ID,
    new_URLStr,
)

starting_page = URL("https://www.cse.ust.hk")
database_path = Path("crawled.json")
result_path = Path("spider_result.txt")
number_of_pages = 50
word_regex = compile(r"[a-zA-Z0-9\-_]+")


async def main() -> None:
    async with await database_path.open("a+t") as database_file:
        database = Database(database_file)
        try:
            database_obj = await database.read()
        except Database.InvalidFormat:
            await database.clear()
            database_obj: object = {}
        database_obj = Scheme.fix(database_obj)

        async with Crawler() as crawler:

            async def crawl_ok_responses():
                await crawler.enqueue(starting_page)
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
                if pages_indexed >= number_of_pages:
                    break
                url_str = new_URLStr(response.url)
                url_id = database_obj["url_ids"].setdefault(url_str, gen_ID(URLID))
                page = database_obj["pages"].setdefault(
                    url_id,
                    Scheme.Page(
                        {
                            "title": "",
                            "text": "",
                            "links": [],
                            "mod_time": NULL_TIME,
                        }
                    ),
                )
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
                    mod_time = NULL_TIME
                if mod_time != NULL_TIME and mod_time <= page["mod_time"]:
                    continue
                html = BeautifulSoup(await response.text(), "html.parser")
                page.update(
                    {
                        "title": "" if html.title is None else html.title.string or "",
                        "text": html.text,
                        "links": list(map(new_URLStr, outbound_urls)),
                        "mod_time": mod_time,
                    }
                )
                forward_index_page = database_obj["forward_index"].setdefault(
                    url_id, {}
                )
                for match in word_regex.finditer(html.text):
                    position, word = WordPosition(match.start()), Word(match[0])
                    word_id = database_obj["word_ids"].setdefault(word, gen_ID(WordID))

                    inverted_index_word_page = (
                        database_obj["inverted_index"]
                        .setdefault(word_id, {})
                        .setdefault(url_id, [])
                    )
                    insert_index = bisect_right(inverted_index_word_page, position)
                    if (
                        insert_index >= 1
                        and inverted_index_word_page[insert_index - 1] == position
                    ):
                        continue

                    inverted_index_word_page.insert(insert_index, position)
                    forward_index_page[word_id] = WordFrequency(
                        forward_index_page.setdefault(word_id, WordFrequency(0)) + 1
                    )

        await database.write(database_obj)

    database_obj = Scheme.hydrate(database_obj)
    async with await result_path.open("wt") as result_file:
        for url_id, page in database_obj["pages"].items():
            await result_file.write(page["title"] or "(no title)")
            await result_file.write("\n")
            await result_file.write(database_obj["urls"][url_id])
            await result_file.write("\n")
            await result_file.write(str(page["mod_time"] or "(no modification time)"))
            await result_file.write(", ")
            await result_file.write(
                str(len(page["text"])) if page["text"] else "(no text)"
            )
            await result_file.write("\n")
            for word_id, frequency in islice(
                database_obj["forward_index"][url_id].items(), 10
            ):
                await result_file.write(database_obj["words"][word_id])
                await result_file.write(" ")
                await result_file.write(str(frequency))
                await result_file.write("; ")
            await result_file.write("\n")
            for link in islice(page["links"], 10):
                await result_file.write(link)
                await result_file.write("\n")
            await result_file.write("\n")


def parser(parent: Callable[..., ArgumentParser] | None = None):
    prog = modules[__name__].__package__ or __name__
    parser = (ArgumentParser if parent is None else parent)(
        prog=f"python -m {prog}",
        description="crawl the Internet",
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
