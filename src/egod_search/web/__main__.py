from asyncio import run
from sys import argv

from .main import parser


def main() -> None:
    """
    Main program.
    """
    entry = parser().parse_args(argv[1:])
    run(entry.invoke(entry))


if __name__ in {"__main__", "__mp_main__"}:
    main()
