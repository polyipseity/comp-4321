# -*- coding: UTF-8 -*-
import builtins
from functools import wraps
from io import StringIO
from traceback import format_exception
from main import layout  # type: ignore
from nicegui import ui


@ui.refreshable
def eval_code(code: str) -> None:
    patched_builtins = builtins.__dict__.copy()

    old_print = patched_builtins["print"]
    stdout = StringIO()

    @wraps(old_print)
    def print(*args: object, **kwargs: object):
        old_print(file=stdout, *args, **kwargs)

    patched_builtins["print"] = print

    try:
        exec(code, {"__builtins__": patched_builtins})
    except BaseException as exc:
        for line in format_exception(exc):
            ui.label(line)
        if not isinstance(exc, Exception):
            raise
    else:
        ui.label(stdout.getvalue())


@ui.page("/debug")
def debug() -> None:
    layout("Debug")

    with ui.row():
        code_textarea = ui.textarea(label="Evaluate Python code")
        ui.button(icon="send", on_click=lambda: eval_code.refresh(code_textarea.value))
    eval_code("")
