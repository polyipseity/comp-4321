# -*- coding: UTF-8 -*-
# macOS packaging support: https://nicegui.io/documentation/section_configuration_deployment#macos_packaging
from multiprocessing import freeze_support  # noqa
freeze_support()  # noqa

from anyio import Path
from argparse import ArgumentParser, Namespace
from egod_search import VERSION
from egod_search.database.models import default_config
from json import dumps, load
from logging import INFO, basicConfig, getLogger
from nicegui import app, ui
from pathlib import Path as SyncPath
from subprocess import run
from sys import executable, exit, stderr, stdin, stdout
from tortoise import Tortoise
from typing import Callable

_CONFIGURATION_PATH = Path("web_args.json")
_PROGRAM = __package__ or __name__

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

async def on_startup() -> None:
    """
    On server startup.
    """
    await Tortoise.init( # type: ignore
        default_config(f"sqlite{DATABASE_PATH.as_uri()[len('file'):]}"),
    )
    await Tortoise.generate_schemas()

async def on_shutdown() -> None:
    """
    On server shutdown.
    """
    await Tortoise.close_connections()

def layout(title: str) -> None:
    """
    Global page layout.
    """
    ui.page_title(f"{title} | E-God Search")
    with ui.header():
        ui.button(on_click=lambda: left_drawer.toggle(),icon="menu")
        ui.label("E-God Search").tailwind.font_size("4xl").font_weight("light")
        with ui.menu():
            ui.menu_item("Home", on_click=lambda:ui.navigate.to("/"), auto_close=True)
    with ui.left_drawer(value=False, fixed=False, bordered=True, elevated=True) as left_drawer:
        ui.button("Home", on_click=lambda:ui.navigate.to("/"))
        ui.button("Search", on_click=lambda:ui.navigate.to("/search"))

@ui.page("/")
def index():
    """
    Index page.
    """
    layout("Home")

if __name__ in {"__main__", "__mp_main__"}:
    with SyncPath(_CONFIGURATION_PATH).open("rt") as config_file:
        config = load(config_file)

    basicConfig(level=INFO)
    LOGGER = getLogger(_PROGRAM)
    LOGGER.info(f"config: {dumps(config, ensure_ascii=False, indent="\t", sort_keys=True)}")

    DATABASE_PATH = SyncPath(config["database_path"]).resolve()

    app.on_startup(on_startup)  # type: ignore
    app.on_shutdown(on_shutdown)  # type: ignore
    import _search # type: ignore
    ui.run()
