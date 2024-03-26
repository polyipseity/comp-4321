# -*- coding: UTF-8 -*-
from pathlib import Path
from PyInstaller.__main__ import run

from . import DIRECTORY, NAME


def main():
    cwd = Path(__file__).parent
    run(
        (
            "--add-data",
            f"{cwd / 'database' / 'create_database.sql'}:{DIRECTORY}/database/create_database.sql",
            "--name",
            NAME,
            "--nowindowed",
            "--onefile",
            str(cwd / "_main.py"),
        )
    )


if __name__ == "__main__":
    main()
