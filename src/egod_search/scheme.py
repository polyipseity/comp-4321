# -*- coding: UTF-8 -*-
from collections import defaultdict
from random import randint
from typing import (
    Any,
    Callable,
    Iterator,
    MutableMapping,
    MutableSequence,
    NewType,
    TypeVar,
    TypedDict,
)
from yarl import URL

_T = TypeVar("_T")

ID = NewType("ID", int)
Time = NewType("Time", int)
URLID = NewType("URLID", ID)
URLStr = NewType("URLStr", str)
Word = NewType("Word", str)
WordFrequency = NewType("WordFrequency", int)
WordID = NewType("WordID", ID)
WordPosition = NewType("WordPosition", int)

NULL_TIME = Time(-1)


def gen_ID(type: Callable[[ID], _T] = ID) -> _T:
    """
    Generate a new ID.
    """
    return type(ID(randint(0, 2**64 - 1)))


def new_URLStr(url_str: str) -> URLStr:
    """
    Normalize a URL string.
    """
    return URLStr(str(URL(url_str)))


def _RecursiveStrDefaultDict() -> MutableMapping[str, Any]:
    return defaultdict(_RecursiveStrDefaultDict)


def _try_get(obj: Any, key: Any, default: Any = {}) -> Any:
    try:
        return obj[key]
    except Exception:
        return default


def _try_iter(obj: Any, default: Iterator[Any] = iter(())) -> Iterator[Any]:
    try:
        return iter(obj)
    except Exception:
        return default


class Scheme:
    class Database(TypedDict):
        URL_IDs: MutableMapping[URLStr, URLID]
        word_IDs: MutableMapping[Word, WordID]

        pages: MutableMapping[URLID, "Scheme.Page"]

        forward_index: MutableMapping[URLID, MutableMapping[WordID, WordFrequency]]
        inverted_index: MutableMapping[
            WordID, MutableMapping[URLID, MutableSequence[WordPosition]]
        ]

    class Page(TypedDict):
        title: str
        text: str
        links: MutableSequence[URLStr]
        mod_time: Time

    @classmethod
    def fix(cls, obj: Any) -> tuple["Scheme.Database", MutableMapping[str, Any]]:
        """
        Convert an object to the scheme format.
        """

        valid = Scheme.Database(
            {
                "URL_IDs": {},
                "word_IDs": {},
                "pages": {},
                "forward_index": {},
                "inverted_index": {},
            }
        )
        invalid = _RecursiveStrDefaultDict()

        cur_obj = _try_get(obj, "URL_IDs")
        cur_IDs = set[URLID]()
        for key in _try_iter(cur_obj):
            try:
                val = cur_obj[key]
            except Exception:
                invalid["URL_IDs"][repr(key)] = None
                continue
            if not (isinstance(key, str) and isinstance(val, int)):
                invalid["URL_IDs"][repr(key)] = repr(val)
                continue
            key_fixed, val_fixed = new_URLStr(key), URLID(ID(val))
            if key_fixed in valid["URL_IDs"]:
                invalid["URL_IDs"][key] = val
                continue
            if val_fixed in cur_IDs:
                invalid["URL_IDs"][key] = val
                while val_fixed in cur_IDs:
                    val_fixed = gen_ID(URLID)
            cur_IDs.add(val_fixed)
            valid["URL_IDs"][key_fixed] = val_fixed
        valid_URL_IDs = cur_IDs

        cur_obj = _try_get(obj, "word_IDs")
        cur_IDs = set[WordID]()
        for key in _try_iter(cur_obj):
            try:
                val = cur_obj[key]
            except Exception:
                invalid["word_IDs"][repr(key)] = None
                continue
            if not (isinstance(key, str) and isinstance(val, int)):
                invalid["word_IDs"][repr(key)] = repr(val)
                continue
            key_fixed, val_fixed = Word(key), WordID(ID(val))
            if val_fixed in cur_IDs:
                invalid["word_IDs"][key] = val
                while val_fixed in cur_IDs:
                    val_fixed = gen_ID(WordID)
            cur_IDs.add(val_fixed)
            valid["word_IDs"][key_fixed] = val_fixed
        valid_word_IDs = cur_IDs

        def fix_page(obj: Any) -> Scheme.Page | None:
            cur_obj = _try_get(obj, "text", ...)
            if cur_obj is ...:
                return None
            text = repr(cur_obj)

            cur_obj = _try_get(obj, "links", [])
            links = list(new_URLStr(repr(element)) for element in _try_iter(cur_obj))

            return Scheme.Page(
                {
                    "title": _try_get(obj, "title", ""),
                    "text": text,
                    "links": links,
                    "mod_time": _try_get(obj, "mod_time", NULL_TIME),
                }
            )

        cur_obj = _try_get(obj, "pages")
        for key in _try_iter(cur_obj):
            try:
                val = cur_obj[key]
            except Exception:
                invalid["pages"][repr(key)] = None
                continue
            try:
                key = URLID(ID(int(key)))
            except TypeError:
                invalid["pages"][repr(key)] = repr(val)
                continue
            if key not in valid_URL_IDs:
                invalid["pages"][key] = repr(val)
                continue
            val_fixed = fix_page(val)
            if val_fixed is None:
                invalid["pages"][key] = repr(val)
                continue
            valid["pages"][key] = val_fixed

        return valid, invalid
