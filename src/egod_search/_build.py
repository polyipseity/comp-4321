from pathlib import Path
from PyInstaller.__main__ import run

from . import NAME, PACKAGE_NAME


def main() -> None:
    """
    Build a native executable from this module.
    """
    cwd = Path(__file__).parent
    run(
        (
            "--add-data",
            f"{(cwd / 'res').__fspath__()}:{PACKAGE_NAME}/res",
            "--name",
            NAME,
            "--nowindowed",
            "--onefile",
            (cwd / "_main.py").__fspath__(),
        )
    )


if __name__ == "__main__":
    main()
