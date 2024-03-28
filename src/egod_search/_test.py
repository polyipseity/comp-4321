# -*- coding: UTF-8 -*-
from sys import modules
from unittest import main as test_main

from . import PACKAGE_NAME


def main() -> None:
    """
    Test this module.
    """
    for key in tuple(modules):
        if key.startswith(PACKAGE_NAME):
            del modules[key]  # reload this package except this module to export tests
    test_main(module=PACKAGE_NAME)


if __name__ == "__main__":
    main()
