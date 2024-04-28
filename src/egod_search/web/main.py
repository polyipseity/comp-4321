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
