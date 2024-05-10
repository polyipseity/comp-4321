# -*- coding: UTF-8 -*-
from typing import Any, Sequence
from egod_search.database.models import MODELS, Page, WordPositionsType
from egod_search.query import lex_query, parse_query
from egod_search.retrieve.search import SearchResultsDebug, search_terms_phrases
from main import layout  # type: ignore
from math import sqrt
from nicegui import ui
from numpy.linalg import norm


def cosine_distance(list1: Sequence[float], list2: Sequence[float]):
    """if len(list1) == 1:
    return 1 / list1[0]"""

    # Calculate dot product
    dot_product = sum(x * y for x, y in zip(list1, list2))

    # Calculate magnitudes
    magnitude_list1 = sqrt(sum(x**2 for x in list1))
    magnitude_list2 = sqrt(sum(x**2 for x in list2))

    # Calculate cosine distance
    cosine_distance = 1 - (dot_product / (magnitude_list1 * magnitude_list2))

    if abs(cosine_distance) < 1e-9:
        return 0

    return cosine_distance


def magnitude_of_list(list1: Sequence[float]):
    return sqrt(sum(x**2 for x in list1))


def _show_tf_idf(
    results: SearchResultsDebug | None,
    type: WordPositionsType = WordPositionsType.PLAINTEXT,
):
    if results is None:
        return
    with ui.dialog() as dialog, ui.card():
        with ui.carousel(animated=True, arrows=True, navigation=True).props(
            "control-color=black"
        ):
            stem_idx_map = {stem: idx for idx, stem in enumerate(results.stems)}
            for term, stem in results.terms.items():
                with ui.carousel_slide():
                    ui.label(f'For term "{term}":')
                    if stem is None:
                        ui.label("This search term does not have a stem")
                        continue
                    stem_idx = stem_idx_map[stem]
                    ui.label(f"Stem: {stem.content}")
                    ui.label(f"Document Frequency (DF): {results.idf_raw[stem_idx]}")
                    ui.label(
                        f"Inverse Document Frequency (IDF): {results.idf[stem_idx]}"
                    )
                    columns = [
                        {
                            "name": "id",
                            "label": "Page ID",
                            "field": "id",
                            "required": True,
                            "align": "left",
                            "sortable": True,
                        },
                        {
                            "name": "tf",
                            "label": "TF",
                            "field": "tf",
                            "sortable": True,
                        },
                        {
                            "name": "tfnorm",
                            "label": "TF (norm.)",
                            "field": "tfnorm",
                            "sortable": True,
                        },
                        {
                            "name": "tfxidf_div_maxtf",
                            "label": "TFxIDF/max(TF)",
                            "field": "tfxidf_div_maxtf",
                            "sortable": True,
                        },
                    ]

                    match type:
                        case WordPositionsType.PLAINTEXT:
                            tf, tf_norm, tf_idf = (
                                results.tf,
                                results.tf_normalized,
                                results.tf_idf,
                            )
                        case WordPositionsType.TITLE:
                            tf, tf_norm, tf_idf = (
                                results.tf_title,
                                results.tf_normalized_title,
                                results.tf_idf_title,
                            )
                        case _:  # type: ignore
                            raise ValueError(type)
                    ui.table(
                        columns=columns,
                        row_key="id",
                        rows=[
                            {
                                "id": page.id,
                                "tf": tf[page_idx][stem_idx],
                                "tfnorm": tf_norm[page_idx][stem_idx],
                                "tfxidf_div_maxtf": tf_idf[page_idx][stem_idx],
                            }
                            for page_idx, page in enumerate(results.pages)
                        ],
                    )
    ui.button(
        f"TFxIDF/max(TF){' (title)' if type == WordPositionsType.TITLE else ''}",
        on_click=dialog.open,
    )


@ui.refreshable
def show_tf_idf(*args: Any, **kwargs: Any):
    """
    Shows TF–IDF details.
    """
    _show_tf_idf(*args, **kwargs)


@ui.refreshable
def show_tf_idf_title(*args: Any, **kwargs: Any):
    """
    Shows TF–IDF details. For titles.
    """
    _show_tf_idf(type=WordPositionsType.TITLE, *args, **kwargs)


@ui.refreshable
def show_vector_space(results: SearchResultsDebug | None):
    """
    Show vector space details.
    """
    if results is None:
        return
    columns = [
        {
            "name": "__id",
            "label": "Page ID",
            "field": "__id",
            "required": True,
            "align": "left",
        },
        {
            "name": "__cos",
            "label": "Cosine Similarity",
            "field": "__cos",
            "required": True,
            "align": "left",
            "sortable": True,
        },
        {
            "name": "__mag",
            "label": "Magnitude (tirbreaker)",
            "field": "__mag",
            "required": True,
            "align": "left",
            "sortable": True,
        },
    ]
    for stem in results.stems:
        stem_word = stem.content
        columns.append(
            {
                "name": stem_word,
                "label": stem_word,
                "field": stem_word,
                "sortable": True,
            }
        )
    with ui.dialog().classes("w-full") as dialog, ui.card():
        ui.table(
            columns=columns,
            row_key="__id",
            rows=[
                {
                    "__id": page.id,
                    "__cos": results.weights[page_idx],
                    "__mag": norm(results.tf_idf[page_idx]),  # TODO: consider title,
                    **dict(
                        zip(
                            (stem.content for stem in results.stems),
                            results.tf_idf[page_idx],
                            strict=True,
                        )
                    ),
                }
                for page_idx, page in enumerate(results.pages)
            ],
        )
    ui.button("Vector Space", on_click=dialog.open)


def _show_page(page: Page):
    # Remember to prefetch/fetch `url`
    with ui.card().classes("w-full"):
        with ui.row().classes("w-full"):
            ui.label("1").classes("text-4xl")
            ui.label(page.title).classes("text-2xl")
            ui.link(page.url.content, target=page.url.content)
            ui.label(f"Size: {page.size}")
            ui.label(f"Time: {page.mod_time.isoformat()}")
        with ui.column().classes("w-full"):
            with ui.scroll_area().classes("w-full h-32 border"):
                ui.label(page.plaintext[:339])


@ui.refreshable
def show_pages(pages: Sequence[Page] | None, *, pagination_index: int = 1):
    """
    Show all pages. Supports pagination.

    `pagination_index` is 1-based.
    """
    if pages is None:
        return
    maximum_items_in_page = 50
    how_many_pages = len(pages) // maximum_items_in_page + 1
    p = ui.pagination(
        1,
        how_many_pages,
        value=pagination_index,
        direction_links=True,
        on_change=lambda: show_pages.refresh(p.value),
    )

    def binder(pagination_index: int):
        return f"{len(pages)} results; page {pagination_index}: Results {(pagination_index - 1) * maximum_items_in_page + 1} - {min(pagination_index * maximum_items_in_page, len(pages))}"

    ui.label().bind_text_from(p, "value", binder)
    # show only page in the pagination of 50 pages per tab

    # print the list index for debugging
    # print("Page", pagination_index)
    # print((pagination_index - 1) * maximum_items_in_page, pagination_index * maximum_items_in_page)
    partitioned_pages = pages[
        (pagination_index - 1)
        * maximum_items_in_page : pagination_index
        * maximum_items_in_page
    ]
    # print(partitioned_results)
    for result in partitioned_pages:
        _show_page(result)


@ui.page("/search")
def search():
    """
    Search page.
    """

    async def search():
        query_result = parse_query(lex_query(input_field.value))
        indexed_how_many_pages.text = (
            f"Finding from {await MODELS.Page.all().count()} pages"
        )

        search_results = await search_terms_phrases(
            MODELS, query_result.terms, phrases=query_result.phrases, debug=True
        )

        show_tf_idf.refresh(search_results)
        show_tf_idf_title.refresh(search_results)
        show_vector_space.refresh(search_results)
        show_pages.refresh(search_results.pages, pagination_index=1)

    layout("Search")

    # create a full-width ui.card for fitting the input field
    with ui.card().classes("w-full"):
        # create row that fills available space
        with ui.row().classes("w-full items-center"):
            input_field = ui.input("Search query").classes("grow")
            # fix the submission_onclick not a local variable error
            ui.button("Submit", on_click=search)
    indexed_how_many_pages = ui.label("Input query and press submit to search")
    with ui.row().classes("w-full items-center"):
        ui.label("Calculations: ")
        show_tf_idf(None)
        show_tf_idf_title(None)
        show_vector_space(None)
    show_pages(None)
    # show_pages.refresh()
