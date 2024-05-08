# -*- coding: UTF-8 -*-
from json import dumps, load
from logging import INFO, basicConfig, getLogger
from os import PathLike
from sys import executable, exit, stderr, stdin, stdout
from anyio import Path
from argparse import ArgumentParser, Namespace
from functools import wraps
from typing import Callable
from nicegui import app, ui
from pathlib import Path as SyncPath
from subprocess import run
from tortoise import Tortoise
from tortoise.functions import Sum, Count

from math import log2

from egod_search import VERSION
from egod_search.database.models import APP_NAME, MODELS

from egod_search.index.transform import porter

_CONFIGURATION_PATH = Path("web_args.json")
_PROGRAM = __package__ or __name__
_LOGGER = getLogger(_PROGRAM)

import math

def cosine_distance(list1, list2):
    if len(list1) == 1:
        return 1/list1[0]

    # Calculate dot product
    dot_product = sum(x * y for x, y in zip(list1, list2))

    # Calculate magnitudes
    magnitude_list1 = math.sqrt(sum(x**2 for x in list1))
    magnitude_list2 = math.sqrt(sum(x**2 for x in list2))

    # Calculate cosine distance
    cosine_distance = 1 - (dot_product / (magnitude_list1 * magnitude_list2))

    return cosine_distance


def main(*, database_path: PathLike[str]) -> None:
    """
    Main program.
    """
    basicConfig(level=INFO)
    _LOGGER.info(
        f"""arguments: {({
        'database_path': database_path,
        })}"""
    )

    async def on_startup():
        await Tortoise.init(  # type: ignore
            {
                "apps": {
                    APP_NAME: {
                        "default_connection": "default",
                        "models": ("egod_search.database.models",),
                    }
                },
                "connections": {
                    "default": f"sqlite://{database_path.__fspath__()}",
                },
                "routers": (),
                "timezone": "UTC",
                "use_tz": True,
            }
        )
        await Tortoise.generate_schemas()

    async def on_shutdown():
        await Tortoise.close_connections()

    @ui.page("/other_page")
    async def other_page():  # type: ignore
        ui.label("Welcome to the other side")

    @ui.page("/dark_page", dark=True)
    async def dark_page():  # type: ignore
        ui.label("Welcome to the dark side")

    @ui.page("/")
    async def index():  # type: ignore
        ui.label("Welcome to home")
        async for page in MODELS.Page.filter(
            url=await MODELS.URL.get(
                content="https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
            )
        ):
            ui.label(page.text)

    @ui.page("/search")
    async def search_page():
        input_field = ui.input("List of keywords, by comma")

        indexed_how_many_pages = ui.label()


    

        @ui.refreshable
        def show_stems_info(arr_of_dict = None):
            if arr_of_dict is None:
                return
            

            with ui.dialog() as dialog, ui.card():
                with ui.carousel(animated=True, arrows=True, navigation=True).props("control-color=black"):
                    for dict_info in arr_of_dict:
                        with ui.carousel_slide():
                            ui.label(f"For stem word {dict_info['stem']}:")
                            if dict_info.get("notfound", False):
                                ui.label("This stem word is not found!")
                                continue
                            ui.label(f"Document Frequency (DF): {dict_info['df']}")
                            ui.label(f"Inverse Document Frequency (IDF): {dict_info['idf']}")
                            columns = [
                                {'name': 'id', 'label': 'Page ID', 'field': 'id', 'required': True, 'align': 'left'},
                                {'name': 'tf', 'label': 'TF', 'field': 'tf', 'sortable': True},
                                {'name': 'maxtf', 'label': 'max(TF)', 'field': 'maxtf', 'sortable': True},
                                {'name': 'tfxidf_div_maxtf', 'label': 'TFxIDF/max(TF)', 'field': 'tfxidf_div_maxtf', 'sortable': True},
                            ]
                            rows = []
                            for k,v in dict_info['tf_dict'].items():
                                v['id'] = k
                                rows.append(v)

                            print(rows)

                            ui.table(columns=columns, rows=rows, row_key='id')
            ui.button('Show TFxIDF/max(TF) calculation info', on_click=dialog.open)
            




        @ui.refreshable
        def show_vector_space_info(vector_space_dict = None):
            if vector_space_dict is None:
                return
            
            columns = [
                {'name': '__id', 'label': 'Page ID', 'field': '__id', 'required': True, 'align': 'left'},
                
            ]

            for stemwords in list(vector_space_dict.values())[0].keys():
                columns.append({'name': stemwords, 'label': stemwords, 'field': stemwords, 'sortable': True})

            columns.append({'name': '__cos', 'label': 'Cosine Distance', 'field': '__cos', 'required': True, 'align': 'left'})

            rows = []

            for k,v in vector_space_dict.items():
                v['__id'] = k
                rows.append(v)
            print("debug vector space table")
            print(columns)
            print(rows)
            with ui.dialog() as dialog, ui.card():
                ui.table(columns=columns, rows=rows, row_key='id')
            ui.button('Show Vector Space info', on_click=dialog.open)
            


        def show_each_page(each_info):
            with ui.card().classes("w-full"):
                with ui.row().classes("w-full"):
                    output_label_rank = ui.label("1").classes("text-4xl")
                    output_label_title = ui.label(each_info["title"]).classes("text-2xl")
                    output_label_size = ui.label("Size: "+str(each_info["size"]))
                    output_label_time = ui.label("Time: "+str(each_info['mod_time']))
                with ui.column().classes("w-full"):
                    with ui.scroll_area().classes('w-full h-32 border'):
                        output_label_text = ui.label(str(each_info["plaintext"])[:339])

        @ui.refreshable
        def show_all_pages(arr_info=None):
            if arr_info is None:
                return
            for each_info in arr_info:
                show_each_page(each_info)


        async def submission_onclick():
            output_to_call_function = []

            page_ids = [x['id'] for x in await MODELS.Page.all().values("id")]

            indexed_how_many_pages.text = f"Finding from {len(page_ids)} pages"

            for stem_raw in input_field.value.lower().split(","):
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

                each_page_tf = {}

                res = (
                    await MODELS.PageWord.filter(word=mget)
                    .annotate(sum=Sum("frequency"))
                    .group_by("page__id")
                    .values("page__id", "sum")
                )
                for x in res:
                    dict_info["tf_dict"][x["page__id"]] = {"tf":x['sum']}
                    each_page_tf[x["page__id"]] = x["sum"]

                #print(each_page_tf)
                ## max(tf) norm
                page_ids_in_dict = list(each_page_tf.keys())

                for page_id_to_norm in page_ids_in_dict:
                    #print("Begin norm")
                    #print(page_id_to_norm, type(page_id_to_norm))
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

                    #print(res_norm)
                    dict_info["tf_dict"][page_id_to_norm]["maxtf"] = res_norm[0]["frequency"]
                    dict_info["tf_dict"][page_id_to_norm]["tfnorm"] = dict_info["tf_dict"][page_id_to_norm]["tf"] / res_norm[0]["frequency"]
                    each_page_tf[page_id_to_norm] /= res_norm[0]["frequency"]
                    # for x in res:
                #print("After norm")
                #print(each_page_tf)

                word_idf = log2(len(page_ids) / len(each_page_tf))

                dict_info["df"] = len(each_page_tf)

                dict_info["idf"] = log2(len(page_ids) / len(each_page_tf))

                tfxidf = {k: v * word_idf for k, v in each_page_tf.items()}

                for page_id_calc_tfxidf in page_ids_in_dict:
                    dict_info["tf_dict"][page_id_calc_tfxidf]["tfxidf_div_maxtf"] = dict_info["tf_dict"][page_id_calc_tfxidf]["tfnorm"] * dict_info["idf"]
                    
                #print(tfxidf)

                output_to_call_function.append(dict_info)

            all_pages_in_consideration = set()
            for dict_info in output_to_call_function:
                #print(dict_info)
                all_pages_in_consideration = all_pages_in_consideration.union(set(dict_info["tf_dict"].keys()))

            vector_space_dict = {page:{dict_info['stem']:dict_info['tf_dict'].get(page, {}).get("tfxidf_div_maxtf", 0) for dict_info in output_to_call_function} for page in all_pages_in_consideration}

            # filter to only consider full matches

            vector_space_dict = {k:v for k,v in vector_space_dict.items() if not any(x==0 for x in v.values())}

            all_pages_in_consideration_afterfilter = vector_space_dict.keys()

            for page in all_pages_in_consideration_afterfilter:
                vector = list(vector_space_dict[page].values())
                unit_vector = [1 for _ in range(len(vector))]
                vector_space_dict[page]["__cos"] = cosine_distance(vector, unit_vector)

            print("VECTOR SPACE")
            print(vector_space_dict)
            # print(all_pages_in_consideration)
            show_stems_info.refresh(output_to_call_function)
            show_vector_space_info.refresh(vector_space_dict)

            vector_space_dict_sorted = dict(sorted(vector_space_dict.items(), key=lambda x: x[1]['__cos']))
            print("==========")
            print(vector_space_dict_sorted)
            
            for_show_all_pages = []

            for k,v in vector_space_dict_sorted.items():
                res_each_page = await MODELS.Page.filter(id=k).limit(1).values("url__content", "mod_time", "size", "text", "plaintext", "title")
                for_show_all_pages.append({'title':res_each_page[0]['title'], 'size':res_each_page[0]['size'], 'mod_time':res_each_page[0]['mod_time'], 'plaintext':res_each_page[0]['plaintext']})

            show_all_pages.refresh(for_show_all_pages)


        submit_btn = ui.button("Submit", on_click=submission_onclick)
        show_stems_info()
        show_vector_space_info()
        show_all_pages()
        show_all_pages.refresh()

    app.on_startup(on_startup)  # type: ignore
    app.on_shutdown(on_shutdown)  # type: ignore
    ui.run()


def parser(parent: Callable[..., ArgumentParser] | None = None) -> ArgumentParser:
    """
    Create an argument parser suitable for the main program. Pass a parser as `parent` to make this a subparser.
    """
    parser = (ArgumentParser if parent is None else parent)(
        prog=f"python -m {_PROGRAM}",
        description="web interface to search the (crawled) internet",
        add_help=True,
        allow_abbrev=False,
        exit_on_error=False,
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"{_PROGRAM} v{VERSION}",
        help="print version and exit",
    )
    parser.add_argument(
        "-d",
        "--database-path",
        type=Path,
        required=True,
        help="path to database",
    )

    @wraps(main)
    async def invoke(args: Namespace):
        await _CONFIGURATION_PATH.write_text(
            dumps(
                {
                    "database_path": (await args.database_path.resolve()).__fspath__(),
                },
            )
        )
        exit(
            run(  # cannot use async, otherwise reload does not work
                (
                    executable,
                    Path(__file__).parent / "main.py",
                ),
                stderr=stderr,
                stdin=stdin,
                stdout=stdout,
            ).returncode
        )

    parser.set_defaults(invoke=invoke)
    return parser


if __name__ in {"__main__", "__mp_main__"}:
    with SyncPath(_CONFIGURATION_PATH).open("rt") as config_file:
        config = load(config_file)
    main(
        database_path=SyncPath(config["database_path"]).resolve(),
    )
