# -*- coding: UTF-8 -*-
from pathlib import Path
from PyInstaller.__main__ import run

from . import NAME


def main():
    run(
        (
            "--name",
            NAME,
            "--nowindowed",
            "--onefile",
            str(Path(__file__).parent / "_main.py"),
        )
    )


if __name__ == "__main__":
    main()
