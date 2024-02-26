# -*- coding: UTF-8 -*-
from random import randint
from types import EllipsisType
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
    """
    Return `obj` as is if it is a string, otherwise `repr(obj)`.
    """
    return obj if isinstance(obj, str) else repr(obj)


def _try_get(obj: object, key: object, default: object = ...) -> object:
    """
    Return `obj[key]` if possible, otherwise `default`.
    """
    try:
        return obj[key]  # type: ignore
    except Exception:
        return default


def _try_int(obj: object, default: _T = ...) -> int | _T:
    """
    Convert `obj` into an `int` if possible, otherwise `default`.
    """
    try:
        return int(_str_repr(obj))
    except (TypeError, ValueError):
        return default


def _try_iter(obj: object, default: Iterator[object] = iter(())) -> Iterator[object]:
    """
    Get the iterator of `obj` if possible, otherwise `default`.
    """
    try:
        return iter(obj)  # type: ignore
    except Exception:
        return default


class Scheme:
    class Database(TypedDict):
        URL_IDs: MutableMapping[URLStr, URLID]
        word_IDs: MutableMapping[Word, WordID]

        pages: MutableMapping[URLID, "Scheme.Page"]

        inverted_index: MutableMapping[
            WordID, MutableMapping[URLID, MutableSequence[WordPosition]]
        ]
        forward_index: MutableMapping[URLID, MutableMapping[WordID, WordFrequency]]

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
                "inverted_index": {},
                "forward_index": {},
            }
        )

        cur_obj = _try_get(obj, "URL_IDs")
        cur_IDs = set[URLID]()
        for key in _try_iter(cur_obj):
            if (val := _try_get(cur_obj, key)) is ... or (val := _try_int(val)) is ...:
                continue
            key, val = new_URLStr(_str_repr(key)), URLID(ID(val))
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
            if (val := _try_get(cur_obj, key)) is ... or (val := _try_int(val)) is ...:
                continue
            key, val = Word(_str_repr(key)), WordID(ID(val))
            while val in cur_IDs:
                val = gen_ID(WordID)
            cur_IDs.add(val)
            ret["word_IDs"][key] = val
        valid_word_IDs = cur_IDs

        def fix_page(obj: object) -> Scheme.Page | EllipsisType:
            if (cur_obj := _try_get(obj, "text")) is ...:
                return ...
            text = _str_repr(cur_obj)

            return Scheme.Page(
                {
                    "title": _str_repr(_try_get(obj, "title", "")),
                    "text": text,
                    "links": list(
                        map(
                            new_URLStr,
                            map(_str_repr, _try_iter(_try_get(obj, "links"))),
                        )
                    ),
                    "mod_time": Time(
                        _try_int(_try_get(obj, "mod_time", NULL_TIME), -1)
                    ),
                }
            )

        def fix_key_as_URL_ID(key: object) -> URLID | EllipsisType:
            try:
                return URLID(ID(int(_str_repr(key))))
            except (TypeError, ValueError):
                try:
                    return ret["URL_IDs"][new_URLStr(_str_repr(key))]
                except KeyError:
                    return ...

        def fix_key_as_word_ID(key: object) -> WordID | EllipsisType:
            try:
                return WordID(ID(int(_str_repr(key))))
            except (TypeError, ValueError):
                try:
                    return ret["word_IDs"][Word(_str_repr(key))]
                except KeyError:
                    return ...

        cur_obj = _try_get(obj, "pages")
        for key in _try_iter(cur_obj):
            if (key := fix_key_as_URL_ID(key)) is ... or key not in valid_URL_IDs:
                continue
            if (val := _try_get(cur_obj, key)) is ... or (val := fix_page(val)) is ...:
                continue
            ret["pages"][key] = val

        # `forward_index` is generated from `inverted_index`
        cur_obj = _try_get(obj, "inverted_index")
        for key_word_ID in _try_iter(cur_obj):
            if (
                key_word_ID := fix_key_as_word_ID(key_word_ID)
            ) is ... or key_word_ID not in valid_word_IDs:
                continue
            if (val_word_ID := _try_get(cur_obj, key_word_ID)) is ...:
                continue
            ret["inverted_index"][key_word_ID] = {}
            inverted_index_word = ret["inverted_index"][key_word_ID]
            for key_URL_ID in _try_iter(val_word_ID):
                if (
                    key_URL_ID := fix_key_as_URL_ID(key_URL_ID)
                ) is ... or key_URL_ID not in valid_URL_IDs:
                    continue
                if (val_URL_ID := _try_get(val_word_ID, key_URL_ID)) is ...:
                    continue
                inverted_index_word[key_URL_ID] = (
                    inverted_index_word_URL := sorted(
                        set(
                            WordPosition(pos)
                            for pos in map(
                                _try_int, map(_str_repr, _try_iter(val_URL_ID))
                            )
                            if isinstance(pos, int) and pos >= 0
                        )
                    )
                )
                ret["forward_index"][key_URL_ID][key_word_ID] = WordFrequency(
                    len(inverted_index_word_URL)
                )

        return ret
