# -*- coding: UTF-8 -*-
from copy import deepcopy
from types import EllipsisType
from typing import MutableMapping, MutableSequence, TypedDict, cast

from ._util import getitem_or_def, int_or_def, iter_or_def, str_or_repr
from .types import (
    ID,
    URLID,
    Timestamp,
    URLID_gen,
    URLStr,
    URLStr_,
    Word,
    WordFrequency,
    WordID,
    WordID_gen,
    WordPosition,
)


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
        mod_time: Timestamp | None

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

        cur_obj = getitem_or_def(obj, "url_ids")
        cur_IDs = set[URLID]()
        for key in iter_or_def(cur_obj):
            if (val := getitem_or_def(cur_obj, key)) is ... or (
                val := int_or_def(val)
            ) is ...:
                continue
            key, val = URLStr(str_or_repr(key)), URLID(ID(val))
            if key in ret["url_ids"]:
                continue
            while val in cur_IDs:
                val = URLID_gen()
            cur_IDs.add(val)
            ret["url_ids"][key] = val
        valid_url_ids = cur_IDs

        cur_obj = getitem_or_def(obj, "word_ids")
        cur_IDs = set[WordID]()
        for key in iter_or_def(cur_obj):
            if (val := getitem_or_def(cur_obj, key)) is ... or (
                val := int_or_def(val)
            ) is ...:
                continue
            key, val = Word(str_or_repr(key)), WordID(ID(val))
            while val in cur_IDs:
                val = WordID_gen()
            cur_IDs.add(val)
            ret["word_ids"][key] = val
        valid_word_ids = cur_IDs

        def fix_page(obj: object) -> Scheme.Page | EllipsisType:
            if (cur_obj := getitem_or_def(obj, "text")) is ...:
                return ...
            text = str_or_repr(cur_obj)

            mod_time = int_or_def(getitem_or_def(obj, "mod_time"))
            mod_time = None if mod_time is ... else Timestamp(mod_time)

            return Scheme.Page(
                {
                    "title": str_or_repr(getitem_or_def(obj, "title", "")),
                    "text": text,
                    "links": list(
                        map(
                            URLStr,
                            map(str_or_repr, iter_or_def(getitem_or_def(obj, "links"))),
                        )
                    ),
                    "mod_time": mod_time,
                }
            )

        def fix_key_as_url_id(key: object) -> URLID | EllipsisType:
            try:
                return URLID(ID(int(str_or_repr(key))))
            except (TypeError, ValueError):
                try:
                    return ret["url_ids"][URLStr(str_or_repr(key))]
                except KeyError:
                    return ...

        def fix_key_as_word_id(key: object) -> WordID | EllipsisType:
            try:
                return WordID(ID(int(str_or_repr(key))))
            except (TypeError, ValueError):
                try:
                    return ret["word_ids"][Word(str_or_repr(key))]
                except KeyError:
                    return ...

        cur_obj = getitem_or_def(obj, "pages")
        for key in iter_or_def(cur_obj):
            if (key := fix_key_as_url_id(key)) is ... or key not in valid_url_ids:
                continue
            if (val := getitem_or_def(cur_obj, key)) is ... or (
                val := fix_page(val)
            ) is ...:
                continue
            ret["pages"][key] = val

        # `forward_index` is generated from `inverted_index`
        cur_obj = getitem_or_def(obj, "inverted_index")
        for key_word_id in iter_or_def(cur_obj):
            if (
                key_word_id := fix_key_as_word_id(key_word_id)
            ) is ... or key_word_id not in valid_word_ids:
                continue
            if (val_word_id := getitem_or_def(cur_obj, key_word_id)) is ...:
                continue
            ret["inverted_index"][key_word_id] = {}
            inverted_index_word = ret["inverted_index"][key_word_id]
            for key_url_id in iter_or_def(val_word_id):
                if (
                    key_url_id := fix_key_as_url_id(key_url_id)
                ) is ... or key_url_id not in valid_url_ids:
                    continue
                if (val_url_id := getitem_or_def(val_word_id, key_url_id)) is ...:
                    continue
                inverted_index_word[key_url_id] = (
                    inverted_index_word_URL := sorted(
                        set(
                            WordPosition(pos)
                            for pos in map(
                                int_or_def, map(str_or_repr, iter_or_def(val_url_id))
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
