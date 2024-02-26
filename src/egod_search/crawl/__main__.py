# -*- coding: UTF-8 -*-
from asyncio import run
from sys import argv

from .main import parser

if __name__ == "__main__":
    entry = parser().parse_args(argv[1:])
    run(entry.invoke(entry))
