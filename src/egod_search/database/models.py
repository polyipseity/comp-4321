# -*- coding: UTF-8 -*-
from asyncio import gather
from itertools import chain
from typing import NamedTuple, Self, Type, TypeVar, cast
from tortoise import Model
from tortoise.fields import (
    BigIntField,
    CharField,
    DatetimeField,
    Field,
    ForeignKeyField,
    ForeignKeyNullableRelation,
    ForeignKeyRelation,
    ManyToManyField,
    ManyToManyRelation,
    RESTRICT,
    TextField,
)
from tortoise.transactions import atomic
from tortoise.validators import (
    CommaSeparatedIntegerListValidator,
    MinValueValidator,
)

from .. import PACKAGE_NAME
from ..index import IndexedPage

_TExtendsModel = TypeVar("_TExtendsModel", bound=Model)

APP_NAME = PACKAGE_NAME
"""
App name of the models.
"""


class URL(Model):
    """
    A URL.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        app = APP_NAME
        table = "URL"

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

    inlinks: ManyToManyRelation["Page"]
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
        app = APP_NAME
        table = "word"

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    Word ID.
    """

    content = CharField(255, index=True, unique=True)
    """
    The word itself.

    The length limit 255 is used to make it compatible with more database drivers.
    """


class Page(Model):
    """
    An indexed page.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        app = APP_NAME
        table = "page"

    url: ForeignKeyRelation[URL] = ForeignKeyField(
        f"{APP_NAME}.{URL.__name__}", index=True, on_delete=RESTRICT, unique=True
    )
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

    outlinks: ManyToManyRelation[URL] = ManyToManyField(
        f"{APP_NAME}.{URL.__name__}", on_delete=RESTRICT, related_name="inlinks"
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
        old_page = await models.Page.get_or_none(url=url)
        if old_page is not None and old_page.mod_time >= page.mod_time:
            return False

        new_page, _ = await models.Page.update_or_create(  # type: ignore
            {
                "mod_time": page.mod_time,
                "text": page.text,
                "plaintext": page.plaintext,
                "size": page.size,
                "title": page.title,
            },
            url=url,
        )
        await new_page.outlinks.clear()
        await new_page.outlinks.add(*url_map.values())

        # clear index
        await gather(
            models.WordOccurrence.filter(page=new_page).delete(),
            models.WordOccurrenceTitle.filter(page=new_page).delete(),
        )

        # index words
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
        await gather(
            models.WordOccurrence.bulk_create(
                (
                    models.WordOccurrence(
                        page=new_page,
                        word=word_map[word],
                        positions=",".join(map(str, positions)),
                        frequency=len(positions),
                    )
                    for word, positions in page.word_occurrences.items()
                )
            ),
            models.WordOccurrenceTitle.bulk_create(
                (
                    models.WordOccurrenceTitle(
                        page=new_page,
                        word=word_map[word],
                        positions=",".join(map(str, positions)),
                        frequency=len(positions),
                    )
                    for word, positions in page.word_occurrences_title.items()
                )
            ),
        )

        return True


class WordOccurrence(Model):
    """
    A word occurrence on a page.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        app = APP_NAME
        table = "word_occurrence"
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

    positions = TextField(validators=(CommaSeparatedIntegerListValidator(),))
    """
    Positions of the word occurrence on a page.
    """

    frequency = BigIntField()
    """
    Frequency of the word in the page.
    """


class WordOccurrenceTitle(WordOccurrence):
    """
    A word occurrence on a page, for titles.
    """

    class Meta(WordOccurrence.Meta):
        """
        Model metadata.
        """

        abstract = True
        app = APP_NAME
        table = "word_occurrence_title"


class Models(NamedTuple):
    Page: Type[Page]
    URL: Type[URL]
    Word: Type[Word]
    WordOccurrence: Type[WordOccurrence]
    WordOccurrenceTitle: Type[WordOccurrenceTitle]


def new_model(model: type[_TExtendsModel]) -> type[_TExtendsModel]:
    """
    Create a new copy of a model.
    """
    return cast(type[_TExtendsModel], type(model.__name__, (model,), {}))


def new_models(app_name: str = "models") -> Models:
    """
    Create new copies of the models.
    """
    return Models(
        new_model(Page),
        new_model(URL),
        new_model(Word),
        new_model(WordOccurrence),
        new_model(WordOccurrenceTitle),
    )


MODELS = new_models()
"""
Default models.
"""

__models__ = MODELS
"""
Exported models.
"""
