# -*- coding: UTF-8 -*-
from typing import Mapping, Sequence, TypedDict
from egod_search.database.models import MODELS
from egod_search.index.transform import porter
from main import layout  # type: ignore
from math import log2, sqrt
from nicegui import ui
from tortoise.functions import Sum


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


@ui.page("/search")
def search():
    """
    Search page.
    """
    layout("Search")

    input_field = ui.input("Search query")
    indexed_how_many_pages = ui.label()

    class TFDict(TypedDict):
        id: str

    class StemDict(TypedDict):
        stem: str
        df: float
        idf: float
        tf_dict: Mapping[str, TFDict]

    @ui.refreshable
    def show_stems_info(arr_of_dict: Sequence[StemDict] | None = None):
        if arr_of_dict is None:
            return
        with ui.dialog() as dialog, ui.card():
            with ui.carousel(animated=True, arrows=True, navigation=True).props(
                "control-color=black"
            ):
                for dict_info in arr_of_dict:
                    with ui.carousel_slide():
                        ui.label(f"For stem word {dict_info['stem']}:")
                        if dict_info.get("notfound", False):
                            ui.label("This stem word is not found!")
                            continue
                        ui.label(f"Document Frequency (DF): {dict_info['df']}")
                        ui.label(
                            f"Inverse Document Frequency (IDF): {dict_info['idf']}"
                        )
                        columns = [
                            {
                                "name": "id",
                                "label": "Page ID",
                                "field": "id",
                                "required": True,
                                "align": "left",
                            },
                            {
                                "name": "tf",
                                "label": "TF",
                                "field": "tf",
                                "sortable": True,
                            },
                            {
                                "name": "maxtf",
                                "label": "max(TF)",
                                "field": "maxtf",
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
                        rows = list[TFDict]()
                        for k, v in dict_info["tf_dict"].items():
                            v["id"] = k
                            # use a loop to round all integer/float fields in v
                            for key in v.keys():
                                # specifically, tf and maxtf are supposed to be integers
                                if key in ["tf", "maxtf"]:
                                    v[key] = int(v[key])
                                elif key != "id":
                                    v[key] = round(v[key], 5)
                            rows.append(v)

                        
                        print(rows)
                        ui.table(
                            columns=columns,
                            rows=rows,  # type: ignore
                            row_key="id",
                        )
        ui.button("Show TFxIDF/max(TF) calculation info", on_click=dialog.open)

    class VectorSpaceDict(TypedDict):
        __id: str

    @ui.refreshable
    def show_vector_space_info(
        vector_space_dict: Mapping[str, VectorSpaceDict] | None = None
    ):
        if vector_space_dict is None:
            return
        columns = [
            {
                "name": "__id",
                "label": "Page ID",
                "field": "__id",
                "required": True,
                "align": "left",
            },
        ]
        for stemwords in list(vector_space_dict.values())[0].keys():
            # but don't do that if it is "__cos" nor "__mag"
            if stemwords in ["__cos", "__mag"]:
                continue
            columns.append(
                {
                    "name": stemwords,
                    "label": stemwords,
                    "field": stemwords,
                    "sortable": True,
                }
            )
        columns.append(
            {
                "name": "__cos",
                "label": "Cosine Distance",
                "field": "__cos",
                "required": True,
                "align": "left",
                "sortable": True,
            }
        )
        # create also an entry for __mag
        columns.append(
            {
                "name": "__mag",
                "label": "Magnitude (tirbreaker)",
                "field": "__mag",
                "required": True,
                "align": "left",
                "sortable": True,
            }
        )
        rows = list[VectorSpaceDict]()
        for k, v in vector_space_dict.items():
            v["__id"] = k
            # use a loop to round all integer/float fields in v
            for key in v.keys():
                if key != "__id":
                    v[key] = round(v[key], 5)
            rows.append(v)
        print("debug vector space table")
        print(columns)
        print(rows)
        with ui.dialog().classes("w-full") as dialog, ui.card():
            ui.table(
                columns=columns,
                rows=rows,  # type: ignore
                row_key="id",
            )
        ui.button("Show Vector Space info", on_click=dialog.open)

    class Page(TypedDict):
        title: str
        size: str
        mod_time: str
        plaintext: str

    def show_each_page(each_info: Page):
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full"):
                ui.label("1").classes("text-4xl")
                ui.label(each_info["title"]).classes("text-2xl")
                ui.link(each_info["url"], target=each_info["url"])
                ui.label("Size: " + str(each_info["size"]))
                ui.label("Time: " + str(each_info["mod_time"]))
            with ui.column().classes("w-full"):
                with ui.scroll_area().classes("w-full h-32 border"):
                    ui.label(each_info["plaintext"][:339])
    global arr_info_show_all_pages_cache
    arr_info_show_all_pages_cache = None
    @ui.refreshable
    def show_all_pages(arr_info: Sequence[Page] | None = None, page = None):
        global arr_info_show_all_pages_cache
        if arr_info is not None:
            arr_info_show_all_pages_cache = arr_info
        if arr_info_show_all_pages_cache is None:
            return
        if page == None:
            page = 1
        arr_info_show_all_pages_cache = arr_info
        ui.label(f"{len(arr_info)} results")
        maximum_items_in_page = 50
        how_many_pages = len(arr_info) // maximum_items_in_page + 1
        p = ui.pagination(1, how_many_pages, value=page, direction_links=True, on_change=lambda x: show_all_pages.refresh(arr_info_show_all_pages_cache, p.value))
        ui.label().bind_text_from(p, 'value', lambda v: f'Page {v}: Results {(v - 1) * maximum_items_in_page + 1} - {min(v * maximum_items_in_page, len(arr_info))}')
        # show only page in the pagination of 50 pages per tab

        #print the list index for debugging
        print("Page", page)
        print((page - 1) * maximum_items_in_page,page * maximum_items_in_page)
        arr_info_pertab = arr_info[(page - 1) * maximum_items_in_page:page * maximum_items_in_page]
        print(arr_info_pertab)
        for each_info in arr_info_pertab:
            show_each_page(each_info)

    async def submission_onclick():
        output_to_call_function = []
        page_ids = [x["id"] for x in await MODELS.Page.all().values("id")]
        indexed_how_many_pages.text = f"Finding from {len(page_ids)} pages"

        input_value_for_processing = input_field.value
        # Look for phrases in the input field which are surrounded by double quotes, which needs special attention. It is stored in phrase variable
        try:
            _, phrase, _ = input_value_for_processing.split('"', 2)
            input_value_for_processing.replace('"', "")
        except:
            phrase = None

        # find raw stem words in the input field splitted by SPACE CHARACTER 
        splitted_terms = input_value_for_processing.split(" ")

        for stem_raw in splitted_terms:
            dict_info = {}
            dict_info["stem_original"] = stem_raw
            stem = porter(stem_raw.strip())
            dict_info["stem"] = stem
            dict_info["tf_dict"] = {}
            try:
                mget = await MODELS.Word.get(content=stem)
            except:
                dict_info["notfound"] = True
                output_to_call_function.append(dict_info)
                continue
            
            
            """each_page_tf = {}
            res = (
                await MODELS.PageWord.filter(word=mget)
                .annotate(sum=Sum("frequency"))
                .group_by("page__id")
                .values("page__id", "sum")
            )
            for x in res:
                dict_info["tf_dict"][x["page__id"]] = {"tf": x["sum"]}
                each_page_tf[x["page__id"]] = x["sum"]
            # print(each_page_tf)
            ## max(tf) norm
            page_ids_in_dict = list(each_page_tf.keys())
            for page_id_to_norm in page_ids_in_dict:
                # print("Begin norm")
                # print(page_id_to_norm, type(page_id_to_norm))
                try:
                    res_norm = (
                        await MODELS.PageWord.filter(page__id=page_id_to_norm)
                        .order_by(
                            "-frequency"  # Supports ordering by related models too.
                            # A ‘-’ before the name will result in descending sort order, default is ascending.
                        )
                        .limit(1)
                        .values("frequency")
                    )
                except Exception as e:
                    import traceback

                    print(traceback.format_exc())
                # print(res_norm)
                dict_info["tf_dict"][page_id_to_norm]["maxtf"] = res_norm[0][
                    "frequency"
                ]
                dict_info["tf_dict"][page_id_to_norm]["tfnorm"] = (
                    dict_info["tf_dict"][page_id_to_norm]["tf"]
                    / res_norm[0]["frequency"]
                )
                each_page_tf[page_id_to_norm] /= res_norm[0]["frequency"]"""
                # for x in res:
            # print("After norm")
            # print(each_page_tf)
            res_get_new_tf = await MODELS.WordPositions.filter(key__word__content = stem).values("tf_normalized", "frequency", "key__page__id")

            for each_elem in res_get_new_tf:
                print(each_elem)
                dict_info["tf_dict"][each_elem["key__page__id"]] = dict_info["tf_dict"].get(each_elem["key__page__id"], {})
                dict_info["tf_dict"][each_elem["key__page__id"]]["tfnorm"] = each_elem["tf_normalized"]
                dict_info["tf_dict"][each_elem["key__page__id"]]["tf"] = each_elem["frequency"]
                dict_info["tf_dict"][each_elem["key__page__id"]]["maxtf"] = each_elem["frequency"] / each_elem["tf_normalized"]

            page_ids_in_dict = list(dict_info["tf_dict"].keys())
            #patching!!!

            df = len(dict_info["tf_dict"].values())
            each_page_tf = ["PATCH"]

            word_idf = log2(len(page_ids) / df)
            dict_info["df"] = df
            dict_info["idf"] = log2(len(page_ids) / df)
            # tfxidf = {k: v * word_idf for k, v in each_page_tf.items()}
            for page_id_calc_tfxidf in page_ids_in_dict:
                dict_info["tf_dict"][page_id_calc_tfxidf]["tfxidf_div_maxtf"] = (
                    dict_info["tf_dict"][page_id_calc_tfxidf]["tfnorm"]
                    * dict_info["idf"]
                )
            # print(tfxidf)
            output_to_call_function.append(dict_info)
        all_pages_in_consideration = set()
        for dict_info in output_to_call_function:
            # print(dict_info)
            all_pages_in_consideration = all_pages_in_consideration.union(
                set(dict_info["tf_dict"].keys())
            )
        vector_space_dict = {
            page: {
                dict_info["stem"]: dict_info["tf_dict"]
                .get(page, {})
                .get("tfxidf_div_maxtf", 0)
                for dict_info in output_to_call_function
            }
            for page in all_pages_in_consideration
        }
        print("VECTOR SPACE BEFORE FILTER")
        print(vector_space_dict)

        if False:
            # filter to only consider full matches
            vector_space_dict = {
                k: v
                for k, v in vector_space_dict.items()
                if not any(x == 0 for x in v.values())
            }
        all_pages_in_consideration_afterfilter = vector_space_dict.keys()
        for page in all_pages_in_consideration_afterfilter:
            vector = list(vector_space_dict[page].values())
            unit_vector = [1 for _ in range(len(vector))]
            vector_space_dict[page]["__cos"] = cosine_distance(vector, unit_vector)
            vector_space_dict[page]["__mag"] = magnitude_of_list(vector)
        print("VECTOR SPACE")
        print(vector_space_dict)
        # print(all_pages_in_consideration)
        show_stems_info.refresh(output_to_call_function)
        show_vector_space_info.refresh(vector_space_dict)
        vector_space_dict_sorted = dict(
            sorted(vector_space_dict.items(), key=lambda x: (x[1]["__cos"], 1/x[1]["__mag"]))
        )
        print("==========")
        print(vector_space_dict_sorted)
        for_show_all_pages = []
        for k, v in vector_space_dict_sorted.items():
            res_each_page = (
                await MODELS.Page.filter(id=k)
                .limit(1)
                .values(
                    "url__content", "mod_time", "size", "text", "plaintext", "title"
                )
            )
            for_show_all_pages.append(
                {
                    "title": res_each_page[0]["title"],
                    "size": res_each_page[0]["size"],
                    "mod_time": res_each_page[0]["mod_time"],
                    "plaintext": res_each_page[0]["plaintext"],
                    "url": res_each_page[0]["url__content"]
                }
            )
        show_all_pages.refresh(for_show_all_pages)

    ui.button("Submit", on_click=submission_onclick)
    show_stems_info()
    show_vector_space_info()
    show_all_pages()
    show_all_pages.refresh()
