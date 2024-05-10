# -*- coding: UTF-8 -*-
from asyncio import gather
from dataclasses import dataclass
from typing import Sequence

from numpy import argsort, float64, ones, take_along_axis
from numpy.typing import NDArray

from . import cosine_similarity_many, tf_idf_many
from ..database.models import Models, Page, Word, WordPositionsType
from ..index.transform import default_transform_word


@dataclass(frozen=True, slots=True, kw_only=True)
class SearchResults:
    """
    Search results. Includes values of intermediate calculations.
    """

    pages: Sequence[Page]
    """
    List of pages, ordered by decreasing relevance to the terms.
    """

    words: Sequence[Word]
    """
    List of search terms, normalized.
    """

    weights: NDArray[float64]
    """
    Page weights.

    A 1D array.
    """

    tf_idf: NDArray[float64]
    """
    Page–word TF–IDF.

    A 2D array. Indexed by (page, word).
    """

    tf_idf_title: NDArray[float64]
    """
    Page–word TF–IDF. For titles.

    A 2D array. Indexed by (page, word).
    """


async def search(models: Models, terms: Sequence[str]) -> SearchResults:
    """
    Search by terms and return all calculations and results.
    """
    words, pages = await gather(
        models.Word.in_bulk(
            frozenset(filter(None, map(default_transform_word, terms))), "content"
        ),
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
    assert page_weights.ndim == 1
    page_weights_indices = argsort(-page_weights, axis=0)

    return SearchResults(
        pages=tuple(pages[arg] for arg in page_weights_indices.flat),
        words=words,
        weights=take_along_axis(page_weights, indices=page_weights_indices, axis=0),
        tf_idf=take_along_axis(tf_idf, indices=page_weights_indices, axis=0),
        tf_idf_title=take_along_axis(
            tf_idf_title, indices=page_weights_indices, axis=0
        ),
    )
