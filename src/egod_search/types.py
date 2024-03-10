# -*- coding: UTF-8 -*-
from random import randint
from typing import TYPE_CHECKING, NewType, NoReturn
from yarl import URL

Timestamp = NewType("Timestamp", int)
"""
Unix timestamp.
"""
ID = NewType("ID", int)
"""
Unique identifier based on integers.
"""
URLID = NewType("URLID", ID)
"""
ID for URLs.
"""
WordID = NewType("WordID", ID)
"""
ID for words.
"""
URLStr_ = _URLStr = NewType("URLStr_", str)
"""
Normalized URL string. Use `URLStr` instead to create me.
"""
Word = NewType("Word", str)
"""
A string that is a word in our database.
"""
WordFrequency = NewType("WordFrequency", int)
"""
Frequency of the word in a page.
"""
WordPosition = NewType("WordPosition", int)
"""
Location of the word in a page.
"""

if not TYPE_CHECKING:

    def URLStr_(_x: object) -> NoReturn:
        raise TypeError("Use `URLStr` instead to create me.")


def URLStr(_x: str | URL) -> _URLStr:
    """
    Create a normalized URL string.
    """
    return _URLStr(str(URL(_x) if _x is str else _x))


def ID_gen() -> ID:
    """
    Generate a new ID.
    """
    return ID(randint(0, 2**64 - 1))


def URLID_gen() -> URLID:
    """
    Generate a new ID for URLs.
    """
    return URLID(ID_gen())


def WordID_gen() -> WordID:
    """
    Generate a new ID for words.
    """
    return WordID(ID_gen())
