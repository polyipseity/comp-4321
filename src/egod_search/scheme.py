# -*- coding: UTF-8 -*-
from random import randint
from typing import (
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


def _str_repr(obj: object) -> str:
    return obj if isinstance(obj, str) else repr(obj)


def _try_get(obj: object, key: object, default: object = ...) -> object:
    try:
        return obj[key] # type: ignore
    except Exception:
        return default


def _try_iter(obj: object, default: Iterator[object] = iter(())) -> Iterator[object]:
    try:
        return iter(obj) # type: ignore
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
    def fix(cls, obj: object) -> "Scheme.Database":
        """
        Convert an object to the scheme format.
        """

        ret = Scheme.Database(
            {
                "URL_IDs": {},
                "word_IDs": {},
                "pages": {},
                "forward_index": {},
                "inverted_index": {},
            }
        )

        cur_obj = _try_get(obj, "URL_IDs")
        cur_IDs = set[URLID]()
        for key in _try_iter(cur_obj):
            try:
                val = cur_obj[key]
            except Exception:
                continue
            if not (isinstance(key, str) and isinstance(val, int)):
                continue
            key, val = new_URLStr(key), URLID(ID(val))
            if key in ret["URL_IDs"]:
                continue
            while val in cur_IDs:
                val = gen_ID(URLID)
            cur_IDs.add(val)
            ret["URL_IDs"][key] = val
        valid_URL_IDs = cur_IDs

        cur_obj = _try_get(obj, "word_IDs")
        cur_IDs = set[WordID]()
        for key in _try_iter(cur_obj):
            try:
                val = cur_obj[key]
            except Exception:
                continue
            if not (isinstance(key, str) and isinstance(val, int)):
                continue
            key, val = Word(key), WordID(ID(val))
            while val in cur_IDs:
                val = gen_ID(WordID)
            cur_IDs.add(val)
            ret["word_IDs"][key] = val
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
                continue
            try:
                key = URLID(ID(int(key)))
            except TypeError:
                continue
            if key not in valid_URL_IDs:
                continue
            val = fix_page(val)
            if val is None:
                continue
            ret["pages"][key] = val

        return ret
