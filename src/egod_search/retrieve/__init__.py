# -*- coding: UTF-8 -*-
from asyncio import gather
from functools import reduce
from numpy import divide, dot, empty, float64, int64, log2, zeros, zeros_like
from numpy.linalg import norm
from numpy.typing import NDArray
from operator import or_
from tortoise.expressions import Q
from tortoise.query_utils import Prefetch
from typing import Any, Literal, Sequence, overload
from ..database.models import Models, Page, Word, WordPositionsType


async def idf_raw_many(
    models: Models,
    words: Sequence[Word],
    *,
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
) -> NDArray[int64]:
    """
    Get the raw inverse document frequencies of many words.

    Returns a 1D array.
    """
    size = len(words)
    if size <= 0:
        # empty `words`
        return empty((0,), dtype=int64)

    WordPositions = type.model(models)
    ret = zeros((size,), dtype=int64)

    word_idx_map = {
        id: idx for idx, id in reversed(tuple(enumerate(word.id for word in words)))
    }
    for wp in (
        await WordPositions.filter(reduce(or_, (Q(key__word=word) for word in words)))
        .prefetch_related(Prefetch("key__word", queryset=models.Word.all().only("id")))
        .only("id", "key_id")
    ):
        ret[word_idx_map[wp.key.word.id]] += 1
    return ret


async def idf_many(
    models: Models,
    *args: Any,
    **kwargs: Any,
) -> NDArray[float64]:
    """
    Get the raw inverse document frequencies of many words.

    Returns a 1D array.
    """
    idf_raw, num = await gather(
        idf_raw_many(models, *args, **kwargs), models.Page.all().count()
    )
    if num <= 0:
        return zeros_like(idf_raw)
    idf_raw[idf_raw <= 0] = num  # after `log2`, becomes 0
    return log2(num / idf_raw)


@overload
async def tf_many(
    models: Models,
    pages: Sequence[Page],
    words: Sequence[Word],
    *,
    normalized: Literal[True] = True,
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
) -> NDArray[float64]: ...


@overload
async def tf_many(
    models: Models,
    pages: Sequence[Page],
    words: Sequence[Word],
    *,
    normalized: Literal[False],
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
) -> NDArray[int64]: ...


@overload
async def tf_many(
    models: Models,
    pages: Sequence[Page],
    words: Sequence[Word],
    *,
    normalized: bool,
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
) -> NDArray[float64 | int64]: ...


async def tf_many(
    models: Models,
    pages: Sequence[Page],
    words: Sequence[Word],
    *,
    normalized: bool = True,
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
) -> NDArray[float64 | int64]:
    """
    Get the normalized or raw term frequencies of many words across many pages.

    Returns a 2D array.
    """
    page_size, word_size = len(pages), len(words)
    if page_size <= 0 or word_size <= 0:
        # empty `pages` or `words`
        return empty((0, 0), dtype=int64)

    WordPositions = type.model(models)
    freq_key = "tf_normalized" if normalized else "frequency"
    ret = zeros((page_size, word_size), dtype=float64 if normalized else int64)

    page_idx_map = {
        id: idx for idx, id in reversed(tuple(enumerate(page.id for page in pages)))
    }
    word_idx_map = {
        id: idx for idx, id in reversed(tuple(enumerate(word.id for word in words)))
    }
    for wp in (
        await WordPositions.filter(
            reduce(or_, (Q(key__page=page) for page in pages))
            & reduce(or_, (Q(key__word=word) for word in words))
        )
        .prefetch_related(
            Prefetch("key__page", queryset=models.Page.all().only("id")),
            Prefetch("key__word", queryset=models.Word.all().only("id")),
        )
        .only("id", "key_id", freq_key)
    ):
        ret[page_idx_map[wp.key.page.id], word_idx_map[wp.key.word.id]] = getattr(
            wp, freq_key
        )
    return ret


async def tf_idf_many(
    models: Models,
    pages: Sequence[Page],
    words: Sequence[Word],
    *,
    normalized: bool = True,
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
) -> NDArray[float64]:
    """
    Get the normalized or raw TF–IDF of many words across many pages.

    Returns a 2D array.
    """
    tf, idf = await gather(
        tf_many(models, pages, words, normalized=normalized, type=type),
        idf_many(models, words, type=type),
    )
    return tf * idf


def cosine_similarity_many(
    query_vector: NDArray[float64],
    page_vectors: NDArray[float64],
) -> NDArray[float64]:
    """
    Finds the cosine similarities between a query vector (1D array) and many page vectors (2D array).

    Returns a 1D array.
    """
    assert query_vector.ndim == 1
    assert page_vectors.ndim == 2
    assert query_vector.shape[0] == page_vectors.shape[1]
    query_norm = norm(query_vector)
    if query_norm <= 0:
        return zeros(page_vectors.shape[:1])
    page_norms = norm(page_vectors, axis=1)
    return divide(
        dot(query_vector, page_vectors.T),
        query_norm * page_norms,
        out=zeros_like(page_norms),
        where=page_norms > 0,
    )
