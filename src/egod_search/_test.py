# -*- coding: UTF-8 -*-
from multiprocessing import cpu_count, dummy
from pathlib import Path
from types import TracebackType
from typing import Self, Type
from unittest.mock import patch
from unittest_parallel.main import main as test_main  # type: ignore

from . import PACKAGE_NAME


def main() -> None:
    """
    Test this module.
    """
    cwd = Path(__file__).parent
    common_options = (
        "--coverage-branch",
        "--level",
        "class",
        "--start-directory",
        PACKAGE_NAME,
        "--top-level-directory",
        (cwd / "../..").__fspath__(),
    )

    # For tests not using `multiprocessing`, which whose names do not end with `_mp`
    test_main(
        (
            *common_options,
            "-k",
            "*.test_*[!_][!m][!p]",
            *(val for length in range(3) for val in ("-k", f"*.test_{'?' * length}")),
        )
    )

    class WithContextManager:
        def __init__(self, dummy: object) -> None:
            self.__dict__ |= dummy.__dict__

        def __enter__(self) -> Self:
            return self

        def __exit__(
            self,
            exc_type: Type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> None:
            pass

    # For tests using `multiprocessing`, which whose names end with `_mp`
    with (
        patch("unittest_parallel.main.multiprocessing", dummy),
        patch.multiple(
            dummy,
            create=True,
            Manager=lambda *args, **kwargs: WithContextManager(dummy),  # type: ignore
            Pool=lambda *args, orig=dummy.Pool, **kwargs: orig(  # type: ignore
                *args,
                **{
                    key: val
                    for key, val in kwargs.items()  # type: ignore
                    if key in {"processes", "initializer", "initargs"}
                },
            ),
            cpu_count=cpu_count,
            get_context=lambda *args, **kwargs: dummy,  # type: ignore
        ),
    ):
        test_main((*common_options, "-k", "*.test_*_mp"))


if __name__ == "__main__":
    main()
