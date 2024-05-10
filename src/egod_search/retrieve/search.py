# -*- coding: UTF-8 -*-
from asyncio import gather
from dataclasses import dataclass
from functools import reduce
from itertools import chain
from operator import or_
from tortoise.expressions import Q
from tortoise.query_utils import Prefetch
from typing import Literal, OrderedDict, Sequence, overload

from numpy import argsort, float64, int64, ones, take, take_along_axis
from numpy.typing import NDArray

from . import cosine_similarity_many, idf_many, idf_raw_many, tf_idf_many, tf_many
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

    terms: OrderedDict[str, Word | None]
    """
    List of search terms mapping to stems, ordered by terms input order.
    """

    stems: Sequence[Word]
    """
    List of stems, in arbitrary order.
    """

    weights: NDArray[float64]
    """
    Page weights.

    A 1D array. Indexed by (page,).
    """

    tf_idf: NDArray[float64]
    """
    Page–stem TF–IDF.

    A 2D array. Indexed by (page, stem).
    """

    tf_idf_title: NDArray[float64]
    """
    Page–stem TF–IDF. For titles.

    A 2D array. Indexed by (page, stem).
    """


@dataclass(frozen=True, slots=True, kw_only=True)
class SearchResultsDebug(SearchResults):
    """
    Debug version of `SearchResults`.
    """

    idf_raw: NDArray[int64]
    """
    Raw inverse document frequencies for stems.

    A 1D array. Indexed by (stem,).
    """

    idf_raw_title: NDArray[int64]
    """
    Raw inverse document frequencies for stems. For titles.

    A 1D array. Indexed by (stem,).
    """

    idf: NDArray[float64]
    """
    Inverse document frequencies for stems.

    A 1D array. Indexed by (stem,).
    """

    idf_title: NDArray[float64]
    """
    Inverse document frequencies for stems. For titles.

    A 1D array. Indexed by (stem,).
    """

    tf: NDArray[int64]
    """
    Page–stem term frequencies.

    A 2D array. Indexed by (page, stem).
    """

    tf_title: NDArray[int64]
    """
    Page–stem term frequencies. For titles.

    A 2D array. Indexed by (page, stem).
    """

    tf_normalized: NDArray[float64]
    """
    Page–stem term frequencies, normalized.

    A 2D array. Indexed by (page, stem).
    """

    tf_normalized_title: NDArray[float64]
    """
    Page–stem term frequencies, normalized. For titles.

    A 2D array. Indexed by (page, stem).
    """


@overload
async def search_terms_phrases(
    models: Models,
    terms: Sequence[str],
    *,
    phrases: Sequence[str] = (),
    debug: Literal[True],
) -> SearchResultsDebug: ...


@overload
async def search_terms_phrases(
    models: Models,
    terms: Sequence[str],
    *,
    phrases: Sequence[str] = (),
    debug: bool = False,
) -> SearchResults | SearchResultsDebug: ...


async def search_terms_phrases(
    models: Models,
    terms: Sequence[str],
    *,
    phrases: Sequence[str] = (),
    debug: bool = False,
) -> SearchResults | SearchResultsDebug:
    """
    Search by terms and phrases and return all calculations and results.
    """
    words = await models.Word.in_bulk(
        frozenset(filter(None, map(default_transform_word, terms))), "content"
    )
    words_stems = OrderedDict((term, words.get(term)) for term in terms)
    words = tuple(words.values())

    # exclude pages not containing the stem
    if words:
        filter2 = reduce(or_, (Q(key__word=word) for word in words))
        wps, wps_title = await gather(
            models.WordPositions.filter(filter2)
            .prefetch_related(
                Prefetch("key__page", queryset=models.Page.all().only("id"))
            )
            .only("id", "key_id"),
            models.WordPositionsTitle.filter(filter2)
            .prefetch_related(
                Prefetch("key__page", queryset=models.Page.all().only("id"))
            )
            .only("id", "key_id"),
        )
        pages = tuple(
            (
                await models.Page.all()
                .prefetch_related("url")
                .in_bulk(
                    frozenset(wp.key.page.id for wp in chain(wps, wps_title)), "id"
                )
            ).values()
        )
    else:
        pages = ()

    # excludes pages not containing exact phrases in content or title
    pages = tuple(
        page
        for page in pages
        if all(phrase in page.title or phrase in page.plaintext for phrase in phrases)
    )

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

    ret = SearchResults(
        pages=tuple(pages[arg] for arg in page_weights_indices.flat),
        terms=words_stems,
        stems=words,
        weights=take_along_axis(page_weights, indices=page_weights_indices, axis=0),
        tf_idf=take(tf_idf, indices=page_weights_indices, axis=0),
        tf_idf_title=take(tf_idf_title, indices=page_weights_indices, axis=0),
    )
    if debug:
        # Multiple `gather` are used so that Python can infer the typings correctly
        (idf_raw, idf_raw_title, idf, idf_title, tf, tf_title), (
            tf_normalized,
            tf_normalized_title,
        ) = await gather(
            gather(
                idf_raw_many(models, words),
                idf_raw_many(models, words, type=WordPositionsType.TITLE),
                idf_many(models, words),
                idf_many(models, words, type=WordPositionsType.TITLE),
                tf_many(models, pages, words, normalized=False),
                tf_many(
                    models, pages, words, normalized=False, type=WordPositionsType.TITLE
                ),
            ),
            gather(
                tf_many(models, pages, words),
                tf_many(models, pages, words, type=WordPositionsType.TITLE),
            ),
        )
        return SearchResultsDebug(
            pages=ret.pages,
            terms=ret.terms,
            stems=ret.stems,
            weights=ret.weights,
            tf_idf=ret.tf_idf,
            tf_idf_title=ret.tf_idf_title,
            idf_raw=idf_raw,
            idf_raw_title=idf_raw_title,
            idf=idf,
            idf_title=idf_title,
            tf=tf,
            tf_title=tf_title,
            tf_normalized=tf_normalized,
            tf_normalized_title=tf_normalized_title,
        )
    return ret
