# -*- coding: UTF-8 -*-
from json import dumps, load
from logging import INFO, basicConfig, getLogger
from os import PathLike
from sys import executable, exit, stderr, stdin, stdout
from anyio import Path
from argparse import ArgumentParser, Namespace
from functools import wraps
from typing import Callable
from nicegui import app, ui
from pathlib import Path as SyncPath
from subprocess import run
from tortoise import Tortoise
from tortoise.functions import Sum, Count

from math import log2

from egod_search import VERSION
from egod_search.database.models import APP_NAME, MODELS

_CONFIGURATION_PATH = Path("web_args.json")
_PROGRAM = __package__ or __name__
_LOGGER = getLogger(_PROGRAM)


def main(*, database_path: PathLike[str]) -> None:
    """
    Main program.
    """
    basicConfig(level=INFO)
    _LOGGER.info(
        f"""arguments: {({
        'database_path': database_path,
        })}"""
    )

    async def on_startup():
        await Tortoise.init(  # type: ignore
            {
                "apps": {
                    APP_NAME: {
                        "default_connection": "default",
                        "models": ("egod_search.database.models",),
                    }
                },
                "connections": {
                    "default": f"sqlite://{database_path.__fspath__()}",
                },
                "routers": (),
                "timezone": "UTC",
                "use_tz": True,
            }
        )
        await Tortoise.generate_schemas()

    async def on_shutdown():
        await Tortoise.close_connections()

    @ui.page("/other_page")
    async def other_page():  # type: ignore
        ui.label("Welcome to the other side")

    @ui.page("/dark_page", dark=True)
    async def dark_page():  # type: ignore
        ui.label("Welcome to the dark side")

    @ui.page("/")
    async def index():  # type: ignore
        ui.label("Welcome to home")
        async for page in MODELS.Page.filter(
            url=await MODELS.URL.get(
                content="https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
            )
        ):
            ui.label(page.text)

    @ui.page("/search")
    async def search_page():
        input_field = ui.input("List of keywords, by comma")
        output_field = ui.label("Output shows here")

        async def submission_onclick():
            word_occurrences = await MODELS.WordOccurrence.all().prefetch_related(
                "page"
            )

            """for word_occurrence in word_occurrences:
                page = word_occurrence['page']
                print(page)"""
            page_ids = list(set(x.page.id for x in word_occurrences))

            output_field.text = ""

            for stem_raw in input_field.value.lower().split(","):
                stem = stem_raw.strip()
                output_field.text += stem
                output_field.text += ": "

                try:
                    mget = await MODELS.Word.get(content=stem)
                except:
                    output_field.text += "Not In DB"
                    continue

                each_page_tf = {}

                res = (
                    await MODELS.WordOccurrence.filter(word=mget)
                    .annotate(sum=Sum("frequency"))
                    .group_by("page__id")
                    .values("page__id", "sum")
                )
                for x in res:
                    each_page_tf[x["page__id"]] = x["sum"]

                print(each_page_tf)
                ## max(tf) norm
                page_ids_in_dict = list(each_page_tf.keys())

                for page_id_to_norm in page_ids_in_dict:
                    print("Begin norm")
                    print(page_id_to_norm, type(page_id_to_norm))
                    try:
                        """res_test = await MODELS.WordOccurrence.filter(
                            page__id=page_id_to_norm
                        ).values("page__url__content", "word__content", "word__id", "frequency")
                        for elem_lots in res_test:
                            print(elem_lots)"""
                        res_norm = (
                            await MODELS.WordOccurrence.filter(page__id=page_id_to_norm)
                            # filter(
                            #    page=MODELS.Page.get(id=page_id_to_norm)
                            # )
                            .annotate(sum=Sum("frequency"))
                            .group_by("word__id")
                            .order_by(
                                "-frequency"  # Supports ordering by related models too.
                                # A ‘-’ before the name will result in descending sort order, default is ascending.
                            )
                            .limit(1)
                            .values("frequency")
                        )
                    except Exception as e:
                        import traceback

                        print(traceback.format_exc())

                    print(res_norm)
                    each_page_tf[page_id_to_norm] /= res_norm[0]["frequency"]
                    # for x in res:
                print("After norm")
                print(each_page_tf)

                """res2 = (
                    await MODELS.WordOccurrence.filter(
                        word=await MODELS.Word.get(content=input_field.value)
                    )
                    .distinct()
                    .values("page__id")
                )

                word_idf = len(res2)"""

                word_idf = log2(len(page_ids) / len(each_page_tf))

                # print(res2)

                tfxidf = {k: v * word_idf for k, v in each_page_tf.items()}

                print(tfxidf)

                output_field.text += "page_tf: "
                output_field.text += str(each_page_tf)
                output_field.text += "word_idf: "
                output_field.text += str(word_idf)
                output_field.text += "\n"

        submit_btn = ui.button("Submit", on_click=submission_onclick)

    app.on_startup(on_startup)  # type: ignore
    app.on_shutdown(on_shutdown)  # type: ignore
    ui.run()


def parser(parent: Callable[..., ArgumentParser] | None = None) -> ArgumentParser:
    """
    Create an argument parser suitable for the main program. Pass a parser as `parent` to make this a subparser.
    """
    parser = (ArgumentParser if parent is None else parent)(
        prog=f"python -m {_PROGRAM}",
        description="web interface to search the (crawled) internet",
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
        "-d",
        "--database-path",
        type=Path,
        required=True,
        help="path to database",
    )

    @wraps(main)
    async def invoke(args: Namespace):
        await _CONFIGURATION_PATH.write_text(
            dumps(
                {
                    "database_path": (await args.database_path.resolve()).__fspath__(),
                },
            )
        )
        exit(
            run(  # cannot use async, otherwise reload does not work
                (
                    executable,
                    Path(__file__).parent / "main.py",
                ),
                stderr=stderr,
                stdin=stdin,
                stdout=stdout,
            ).returncode
        )

    parser.set_defaults(invoke=invoke)
    return parser


if __name__ in {"__main__", "__mp_main__"}:
    with SyncPath(_CONFIGURATION_PATH).open("rt") as config_file:
        config = load(config_file)
    main(
        database_path=SyncPath(config["database_path"]).resolve(),
    )
