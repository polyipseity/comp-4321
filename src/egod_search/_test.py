# -*- coding: UTF-8 -*-
from os import chdir
from pathlib import Path
from unittest import main as test_main


def main() -> None:
    """
    Test this module.
    """
    chdir(Path(__file__).parent)
    test_main(module=None)


if __name__ == "__main__":
    main()
