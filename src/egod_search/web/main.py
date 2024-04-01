# -*- coding: UTF-8 -*-
from json import dumps, load
from logging import INFO, basicConfig, getLogger
from sys import executable, stderr, stdin, stdout
from aiosqlite import Connection, connect
from anyio import Path
from argparse import ArgumentParser, Namespace
from functools import wraps
from typing import Callable
from nicegui import app, ui
from pathlib import Path as SyncPath
from subprocess import run

from egod_search import VERSION

_CONFIGURATION_PATH = Path("web_args.json")
_PROGRAM = __package__ or __name__
_LOGGER = getLogger(_PROGRAM)


def main(*, database_path: Path) -> None:
    """
    Main program.
    """
    basicConfig(level=INFO)
    _LOGGER.info(
        f"""arguments: {({
        'database_path': database_path,
        })}"""
    )

    db: Connection

    async def on_startup():
        nonlocal db
        db = await connect(database_path.__fspath__())

    async def on_shutdown():
        await db.close()

    @ui.page("/other_page")
    async def other_page():  # type: ignore
        ui.label("Welcome to the other side")

    @ui.page("/dark_page", dark=True)
    async def dark_page():  # type: ignore
        ui.label("Welcome to the dark side")

    @ui.page("/")
    async def index():  # type: ignore
        ui.label("Welcome to home")
        async with db.cursor() as cursor:
            async with db.execute(
                "SELECT rowid FROM main.urls WHERE content = 'https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm'"
            ) as cursor:
                rows = await cursor.fetchall()
                ui.label(str(rows))

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
        database_path=Path(config["database_path"]),
    )
