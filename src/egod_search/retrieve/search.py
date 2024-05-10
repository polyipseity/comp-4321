# -*- coding: UTF-8 -*-
from asyncio import gather
from dataclasses import dataclass
from typing import Sequence

from numpy import float64, ones

from . import cosine_similarity_many, tf_idf_many
from ..database.models import Models, WordPositionsType
from ..index.transform import default_transform_word


@dataclass(frozen=True, slots=True, kw_only=True)
class SearchResults:
    """
    Search results. Includes values of intermediate calculations.
    """

    page_ids: Sequence[int]
    """
    List of page IDs, ordered by decreasing relevance to the terms.
    """


async def search(models: Models, terms: Sequence[str]) -> SearchResults:
    """
    Search by terms and return all calculations and results.
    """
    words, pages = await gather(
        models.Word.in_bulk(frozenset(map(default_transform_word, terms)), "content"),
        models.Page.all(),
    )
    words = tuple(words.values())

    query_tf = ones((len(words),), dtype=float64)
    tf_idf, tf_idf_title = await gather(
        tf_idf_many(models, pages, words),
        tf_idf_many(models, pages, words, type=WordPositionsType.TITLE),
    )

    cos_sim = cosine_similarity_many(query_tf, tf_idf)
    cos_sim_title = cosine_similarity_many(query_tf, tf_idf_title)

    page_weights = cos_sim + 3.9 * cos_sim_title
