# -*- coding: UTF-8 -*-
from inspect import isawaitable
from tortoise.expressions import RawSQL
from tortoise.query_utils import Prefetch
from typing import Any, Sequence
from egod_search.database.models import MODELS, Page, WordPositionsType
from egod_search.query import lex_query, parse_query
from egod_search.retrieve.search import SearchResultsDebug, search_terms_phrases
from main import layout  # type: ignore
from nicegui import app, ui

_FLOAT_ROUND_DIGITS = 6


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
                                "tfnorm": round(
                                    tf_norm[page_idx][stem_idx], _FLOAT_ROUND_DIGITS
                                ),
                                "tfxidf_div_maxtf": round(
                                    tf_idf[page_idx][stem_idx], _FLOAT_ROUND_DIGITS
                                ),
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
            "label": "Magnitude (tiebreaker)",
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
                    "__cos": round(results.weights[page_idx], _FLOAT_ROUND_DIGITS),
                    "__mag": round(results.magnitudes[page_idx], _FLOAT_ROUND_DIGITS),
                    **dict(
                        zip(
                            (stem.content for stem in results.stems),
                            (
                                round(tf_idf, _FLOAT_ROUND_DIGITS)
                                for tf_idf in results.tf_idf[page_idx]
                            ),
                            strict=True,
                        )
                    ),
                }
                for page_idx, page in enumerate(results.pages)
            ],
        )
    ui.button("Vector Space", on_click=dialog.open)


async def _show_page(page: Page, score: float, rank: int):
    # Remember to prefetch/fetch `url`
    with ui.card().classes("w-full"):
        with ui.row().classes("w-fullitems-end"):
            ui.label(str(rank)).classes("text-4xl")
            ui.label(page.title).classes("text-2xl")
            ui.link(page.url.content, target=page.url.content)
            ui.label(f"Score: {score}")
            ui.label(f"Size: {page.size}")
            ui.label(f"Last modification time: {page.mod_time.isoformat()}")
            word_separator = ""
            keywords_str = ""
            async for word in (
                MODELS.PageWord.filter(page=page)
                .annotate(
                    frequency=RawSQL(
                        f"coalesce((SELECT frequency FROM {MODELS.WordPositions._meta.db_table} WHERE key_id = {MODELS.PageWord._meta.db_table}.id), 0)"  # type: ignore
                    )
                    + RawSQL(
                        f"coalesce((SELECT frequency FROM {MODELS.WordPositionsTitle._meta.db_table} WHERE key_id = {MODELS.PageWord._meta.db_table}.id), 0)"  # type: ignore
                    ),
                )
                .order_by("-frequency", "word__content")
                .prefetch_related(
                    Prefetch("word", MODELS.Word.all().only("id", "content"))
                )
                .limit(10)
            ):
                frequency = getattr(word, "frequency")
                assert isinstance(frequency, int)
                keywords_str += word_separator
                word_separator = "; "
                keywords_str += f"{word.word.content} {frequency}"
        ui.label(keywords_str)

        with ui.expansion("Inlinks").classes("w-full"), ui.list():
            for outlink in await page.url.inlinks.limit(10).prefetch_related("url"):
                ui.item(outlink.url.content)
        with ui.expansion("Outlinks").classes("w-full"), ui.list():
            for outlink in await page.outlinks.limit(10):
                ui.item(outlink.content)
        ui.separator()
        with ui.column().classes("w-full"):
            with ui.scroll_area().classes("w-full h-32 border"):
                ui.label(page.plaintext[:339])


@ui.refreshable
async def show_pages(
    pages: Sequence[tuple[Page, float]] | None, *, pagination_index: int = 1
):
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
        on_change=lambda: show_pages.refresh(pagination_index=p.value),
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
    for rank, result in enumerate(
        partitioned_pages, (pagination_index - 1) * maximum_items_in_page + 1
    ):
        await _show_page(result[0], result[1], rank)


@ui.page("/search")
async def search(query: str | None = None):
    """
    Search page.
    """

    val = app.storage.user.setdefault("history", [])
    if not isinstance(val, list):
        app.storage.user["history"] = val = []
    val2: list[str] = val

    async def submit_search():
        if not (input := input_field.value):
            return
        val2.append(input)

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
        tmp = show_pages.refresh(
            tuple(zip(search_results.pages, search_results.weights, strict=True)),
            pagination_index=1,
        )
        assert isawaitable(tmp)
        await tmp

    layout("Search")

    # create a full-width ui.card for fitting the input field
    with ui.card().classes("w-full"):
        # create row that fills available space
        with ui.row().classes("w-full items-center"):
            input_field = ui.input("Search query", autocomplete=val2).classes("grow")
            app.storage.user.on_change(  # type: ignore
                lambda: input_field.set_autocomplete(val2)
            )
            # fix the submission_onclick not a local variable error
            ui.button("Submit", on_click=submit_search)
    indexed_how_many_pages = ui.label("Input query and press submit to search")
    with ui.row().classes("w-full items-center"):
        ui.label("Calculations: ")
        show_tf_idf(None)
        show_tf_idf_title(None)
        show_vector_space(None)
    tmp = show_pages(None)
    assert isawaitable(tmp)
    await tmp
    # show_pages.refresh()

    if query is not None:
        input_field.value = query


@ui.page("/history")
def history():
    val = app.storage.user.setdefault("history", [])
    if not isinstance(val, list):
        app.storage.user["history"] = val = []
    val2: list[str] = val

    layout("History")

    checkboxes = list[ui.checkbox]()

    @ui.refreshable
    def checkboxes_list():
        checkboxes.clear()
        with ui.list():
            for term in val2:
                with ui.item():
                    checkboxes.append(ui.checkbox(term))

    checkboxes_list()

    def delete_on_click():
        for cb in checkboxes:
            if cb.value:
                val2.remove(cb.text)
        checkboxes_list.refresh()

    ui.button("Delete", on_click=delete_on_click)

    def merge_and_search():
        queries = list[str]()
        for cb in checkboxes:
            if cb.value:
                queries.append(cb.text)
        ui.navigate.to(f"/search?query={' '.join(queries)}")

    ui.button("Merge & Search", on_click=merge_and_search)
