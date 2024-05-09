# -*- coding: UTF-8 -*-
from asyncio import gather
from itertools import chain
from re import NOFLAG
from typing import NamedTuple, Self, Type, TypeVar, cast
from numpy import array, float64
from tortoise import Model
from tortoise.fields import (
    BigIntField,
    CharField,
    DatetimeField,
    FloatField,
    ForeignKeyField,
    ForeignKeyNullableRelation,
    ForeignKeyRelation,
    ManyToManyField,
    ManyToManyRelation,
    OneToOneField,
    OneToOneRelation,
    OneToOneNullableRelation,
    RESTRICT,
    SET_NULL,
    TextField,
)
from tortoise.transactions import atomic
from tortoise.validators import MaxValueValidator, MinValueValidator, RegexValidator

from .. import NAME
from ..index import IndexedPage

_TExtendsModel = TypeVar("_TExtendsModel", bound=Model)

APP_NAME = NAME
"""
App name of the models.
"""


def default_config(connection: str):
    """
    Default initialization configuration.
    """
    return {
        "apps": {APP_NAME: {"default_connection": "default", "models": (__name__,)}},
        "connections": {"default": connection},
        "routers": (),
        "timezone": "UTC",
        "use_tz": True,
    }


class Page(Model):
    """
    An indexed page.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True

    url: OneToOneRelation["URL"]
    """
    URL of the page.
    """

    mod_time = DatetimeField()
    """
    Last modification time of the page, as reported by the server.
    """

    size = BigIntField(validators=(MinValueValidator(0),))
    """
    Size of the page, as reported by the server.
    """

    text = TextField()
    """
    Content of the page, including markups.
    """

    plaintext = TextField()
    """
    Plaintext content of the page, excluding markups.
    """

    title = TextField()
    """
    Title of the page.
    """

    outlinks: ManyToManyRelation["URL"] = ManyToManyField(
        f"{APP_NAME}.URL", on_delete=RESTRICT, related_name="inlinks"
    )
    """
    Links outgoing from this page.
    """

    @classmethod
    @atomic()
    async def index(cls, models: "Models", page: IndexedPage) -> bool:
        """
        Index an page and return whether the page is actually indexed.
        """
        urls = (str(page.url), *{str(link): ... for link in page.links})
        await models.URL.bulk_create(
            (models.URL(content=url) for url in urls),
            on_conflict=("content",),
            ignore_conflicts=True,
        )
        url_map = await models.URL.in_bulk(urls, "content")
        url = url_map.pop(urls[0])
        await url.fetch_related("page")
        new_page = url.page
        if new_page is not None and new_page.mod_time >= page.mod_time:
            return False

        if new_page is None:
            new_page = models.Page()
        new_page.update_from_dict(  # type: ignore
            {
                "mod_time": page.mod_time,
                "text": page.text,
                "plaintext": page.plaintext,
                "size": page.size,
                "title": page.title,
            }
        )
        await new_page.save()
        await new_page.outlinks.add(*url_map.values())
        url.page = new_page
        await url.save()

        # clear index
        await models.PageWord.filter(page=new_page).delete()

        # create words
        await models.Word.bulk_create(
            (
                models.Word(content=word)
                for word in chain(page.word_occurrences, page.word_occurrences_title)
            ),
            on_conflict=("content",),
            ignore_conflicts=True,
        )
        word_map = await models.Word.in_bulk(
            chain(page.word_occurrences, page.word_occurrences_title), "content"
        )

        # create positions
        wp_max, wp_max_title = await gather(
            models.WordPositions.all().order_by("-id").only("id").first(),
            models.WordPositionsTitle.all().order_by("-id").only("id").first(),
        )

        wp_id_max = 0 if wp_max is None else wp_max.id
        wps_map = {
            word_str: models.WordPositions(
                id=idx, positions=",".join(map(str, wo)), frequency=len(wo), tf=0
            )
            for idx, word_str in enumerate(word_map, wp_id_max + 1)
            for wo in (page.word_occurrences.get(word_str, ()),)
        }
        try:
            wp_max_freq = max(wps_map.values(), key=lambda item: item.frequency)
        except ValueError:
            pass
        else:
            if wp_max_freq.frequency != 0:
                for wp, tf in zip(
                    wps_map.values(),
                    array([wp.frequency for wp in wps_map.values()], dtype=float64)
                    / wp_max_freq.frequency,
                    strict=True,
                ):
                    wp.tf = tf

        wp_id_max_title = 0 if wp_max_title is None else wp_max_title.id
        wps_map_title = {
            word_str: models.WordPositionsTitle(
                id=idx, positions=",".join(map(str, wo)), frequency=len(wo)
            )
            for idx, word_str in enumerate(word_map, wp_id_max_title + 1)
            for wo in (page.word_occurrences_title.get(word_str, ()),)
        }
        try:
            wp_max_freq_title = max(
                wps_map_title.values(), key=lambda item: item.frequency
            )
        except ValueError:
            pass
        else:
            if wp_max_freq_title.frequency != 0:
                for wp, tf in zip(
                    wps_map_title.values(),
                    array(
                        [wp.frequency for wp in wps_map_title.values()], dtype=float64
                    )
                    / wp_max_freq_title.frequency,
                    strict=True,
                ):
                    wp.tf = tf

        await gather(
            models.WordPositions.bulk_create(wps_map.values()),
            models.WordPositionsTitle.bulk_create(wps_map_title.values()),
        )

        # index words
        await models.PageWord.bulk_create(
            models.PageWord(
                page=new_page,
                word=word,
                positions_id=wps_map[word_str].id,
                positions_title_id=wps_map_title[word_str].id,
            )
            for word_str, word in word_map.items()
        )

        return True


class URL(Model):
    """
    A URL.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    URL ID.
    """

    content = CharField(2047, index=True, unique=True)
    """
    The URL itself.

    The length limit 2047 is commonly used in search engines. See <https://stackoverflow.com/a/417184>.
    """

    redirect: ForeignKeyNullableRelation[Self] = ForeignKeyField(
        f"{APP_NAME}.URL", default=None, null=True, on_delete=RESTRICT
    )
    """
    The URL to be redirected from this URL, if any.
    """

    page: OneToOneNullableRelation[Page] = OneToOneField(
        f"{APP_NAME}.{Page.__name__}",
        related_name="url",
        on_delete=SET_NULL,
        null=True,
        default=None,
    )
    """
    Corresponding page, if indexed.
    """

    inlinks: ManyToManyRelation[Page]
    """
    Pages linking to this URL.
    """


class Word(Model):
    """
    An indexed word.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    Word ID.
    """

    content = CharField(255, index=True, unique=True)
    """
    The word itself.

    The length limit 255 is used to make it compatible with more database drivers.
    """


class WordPositions(Model):
    """
    Word positions for a page—word pair.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        indexes = (("key"),)
        unique_together = (("key",),)

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    ID.
    """

    key: OneToOneRelation["PageWord"]
    """
    Corresponding page pair.
    """

    positions = TextField(
        validators=(RegexValidator(r"\A(?:|\d+(?:,\d+)*)\Z", NOFLAG),)
    )
    """
    Positions of the word occurrence on a page.
    """

    frequency = BigIntField(validators=(MinValueValidator(0),))
    """
    Frequency of the word in the page.
    """

    tf = FloatField(validators=(MinValueValidator(0), MaxValueValidator(1)))
    """
    Term frequency in the page, normalized.
    
    Calculated by (number of occurrences in the page / max number of occurrences of a word in the page).
    """


class WordPositionsTitle(WordPositions):
    """
    Word positions for a page—word pair. For titles.
    """

    class Meta(WordPositions.Meta):
        """
        Model metadata.
        """

        abstract = True


class PageWord(Model):
    """
    A page—word pair.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        indexes = (("page", "word"),)
        unique_together = (("page", "word"),)

    page: ForeignKeyRelation[Page] = ForeignKeyField(
        f"{APP_NAME}.{Page.__name__}", index=True, on_delete=RESTRICT
    )
    """
    The page the word is on.
    """

    word: ForeignKeyRelation[Word] = ForeignKeyField(
        f"{APP_NAME}.{Word.__name__}", index=True, on_delete=RESTRICT
    )
    """
    The word.
    """

    positions: OneToOneRelation[WordPositions] = OneToOneField(
        f"{APP_NAME}.{WordPositions.__name__}", related_name="key", on_delete=RESTRICT
    )
    """
    Word positions for plaintext.
    """

    positions_title: OneToOneRelation[WordPositionsTitle] = OneToOneField(
        f"{APP_NAME}.{WordPositionsTitle.__name__}",
        related_name="key",
        on_delete=RESTRICT,
    )
    """
    Word positions for title.
    """


class Models(NamedTuple):
    Page: Type[Page]
    PageWord: Type[PageWord]
    URL: Type[URL]
    Word: Type[Word]
    WordPositions: Type[WordPositions]
    WordPositionsTitle: Type[WordPositionsTitle]


def new_model(model: type[_TExtendsModel]) -> type[_TExtendsModel]:
    """
    Create a new copy of a model.
    """
    return cast(type[_TExtendsModel], type(model.__name__, (model,), {}))


def new_models() -> Models:
    """
    Create new copies of the models.
    """
    return Models(
        new_model(Page),
        new_model(PageWord),
        new_model(URL),
        new_model(Word),
        new_model(WordPositions),
        new_model(WordPositionsTitle),
    )


MODELS = new_models()
"""
Default models.
"""

__models__ = MODELS
"""
Exported models.
"""
