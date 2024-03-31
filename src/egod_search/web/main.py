# -*- coding: UTF-8 -*-
from json import dumps, load
from logging import INFO, basicConfig, getLogger
from sys import executable, stderr, stdin, stdout
from anyio import Path
from argparse import ArgumentParser, Namespace
from functools import wraps
from typing import Callable
from nicegui import ui
from pathlib import Path as SyncPath
from subprocess import run

try:
    from .. import VERSION
except ImportError:
    VERSION = "Lorem ipsum"  # type: ignore

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

    ui.label(f"version: {VERSION}")
    ui.label(f"database_path: {database_path}")
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
                    "VERSION": VERSION,
                    "database_path": (await args.database_path.resolve()).__fspath__(),
                },
            )
        )
        run(
            (
                executable,
                Path(__file__).parent / "main.py",
            ),
            check=True,
            stderr=stderr,
            stdin=stdin,
            stdout=stdout,
        )

    parser.set_defaults(invoke=invoke)
    return parser


if __name__ in {"__main__", "__mp_main__"}:
    with SyncPath(_CONFIGURATION_PATH).open("rt") as config_file:
        config = load(config_file)
    VERSION = config["VERSION"]  # type: ignore
    main(
        database_path=Path(config["database_path"]),
    )
