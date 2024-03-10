# -*- coding: UTF-8 -*-
from copy import deepcopy
from types import EllipsisType
from typing import (
    Iterator,
    MutableMapping,
    MutableSequence,
    TypeVar,
    TypedDict,
    cast,
)

from .types import (
    ID,
    URLID,
    Timestamp,
    Timestamp_NULL,
    URLID_gen,
    URLStr,
    URLStr_,
    Word,
    WordFrequency,
    WordID,
    WordID_gen,
    WordPosition,
)

_T = TypeVar("_T")


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
        url_ids: MutableMapping[URLStr_, URLID]
        word_ids: MutableMapping[Word, WordID]

        pages: MutableMapping[URLID, "Scheme.Page"]

        inverted_index: MutableMapping[
            WordID,
            MutableMapping[
                URLID, MutableSequence[WordPosition]
            ],  # positions are unique and sorted
        ]
        forward_index: MutableMapping[URLID, MutableMapping[WordID, WordFrequency]]

    class Page(TypedDict):
        title: str
        text: str
        links: MutableSequence[URLStr_]
        mod_time: Timestamp

    @classmethod
    def fix(cls, obj: object) -> "Scheme.Database":
        """
        Convert an object to the scheme format.
        """

        ret = Scheme.Database(
            {
                "url_ids": {},
                "word_ids": {},
                "pages": {},
                "inverted_index": {},
                "forward_index": {},
            }
        )

        cur_obj = _try_get(obj, "url_ids")
        cur_IDs = set[URLID]()
        for key in _try_iter(cur_obj):
            if (val := _try_get(cur_obj, key)) is ... or (val := _try_int(val)) is ...:
                continue
            key, val = URLStr(_str_repr(key)), URLID(ID(val))
            if key in ret["url_ids"]:
                continue
            while val in cur_IDs:
                val = URLID_gen()
            cur_IDs.add(val)
            ret["url_ids"][key] = val
        valid_url_ids = cur_IDs

        cur_obj = _try_get(obj, "word_ids")
        cur_IDs = set[WordID]()
        for key in _try_iter(cur_obj):
            if (val := _try_get(cur_obj, key)) is ... or (val := _try_int(val)) is ...:
                continue
            key, val = Word(_str_repr(key)), WordID(ID(val))
            while val in cur_IDs:
                val = WordID_gen()
            cur_IDs.add(val)
            ret["word_ids"][key] = val
        valid_word_ids = cur_IDs

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
                            URLStr,
                            map(_str_repr, _try_iter(_try_get(obj, "links"))),
                        )
                    ),
                    "mod_time": Timestamp(
                        _try_int(_try_get(obj, "mod_time"), Timestamp_NULL)
                    ),
                }
            )

        def fix_key_as_url_id(key: object) -> URLID | EllipsisType:
            try:
                return URLID(ID(int(_str_repr(key))))
            except (TypeError, ValueError):
                try:
                    return ret["url_ids"][URLStr(_str_repr(key))]
                except KeyError:
                    return ...

        def fix_key_as_word_id(key: object) -> WordID | EllipsisType:
            try:
                return WordID(ID(int(_str_repr(key))))
            except (TypeError, ValueError):
                try:
                    return ret["word_ids"][Word(_str_repr(key))]
                except KeyError:
                    return ...

        cur_obj = _try_get(obj, "pages")
        for key in _try_iter(cur_obj):
            if (key := fix_key_as_url_id(key)) is ... or key not in valid_url_ids:
                continue
            if (val := _try_get(cur_obj, key)) is ... or (val := fix_page(val)) is ...:
                continue
            ret["pages"][key] = val

        # `forward_index` is generated from `inverted_index`
        cur_obj = _try_get(obj, "inverted_index")
        for key_word_id in _try_iter(cur_obj):
            if (
                key_word_id := fix_key_as_word_id(key_word_id)
            ) is ... or key_word_id not in valid_word_ids:
                continue
            if (val_word_id := _try_get(cur_obj, key_word_id)) is ...:
                continue
            ret["inverted_index"][key_word_id] = {}
            inverted_index_word = ret["inverted_index"][key_word_id]
            for key_url_id in _try_iter(val_word_id):
                if (
                    key_url_id := fix_key_as_url_id(key_url_id)
                ) is ... or key_url_id not in valid_url_ids:
                    continue
                if (val_url_id := _try_get(val_word_id, key_url_id)) is ...:
                    continue
                inverted_index_word[key_url_id] = (
                    inverted_index_word_URL := sorted(
                        set(
                            WordPosition(pos)
                            for pos in map(
                                _try_int, map(_str_repr, _try_iter(val_url_id))
                            )
                            if isinstance(pos, int) and pos >= 0
                        )
                    )
                )
                ret["forward_index"][key_url_id][key_word_id] = WordFrequency(
                    len(inverted_index_word_URL)
                )

        return ret

    class HydratedDatabase(Database):
        urls: MutableMapping[URLID, URLStr_]
        words: MutableMapping[WordID, Word]

    @classmethod
    def hydrate(cls, scheme: "Scheme.Database") -> "Scheme.HydratedDatabase":
        """
        Copy and hydrate the scheme object.
        """
        ret = cast(Scheme.HydratedDatabase, deepcopy(scheme))
        ret["urls"] = {val: key for key, val in ret["url_ids"].items()}
        ret["words"] = {val: key for key, val in ret["word_ids"].items()}
        return ret
