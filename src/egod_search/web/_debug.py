# -*- coding: UTF-8 -*-
import builtins
from functools import wraps
from inspect import isawaitable
from traceback import print_exc
from main import layout  # type: ignore
from nicegui import ui


@ui.refreshable
async def eval_code(code: str) -> None:
    """
    Component that evaluates code and outputs text.
    """
    patched_builtins = builtins.__dict__.copy()

    old_print = patched_builtins["print"]

    class Stdout:
        def write(self, s: str, /) -> None:
            ui.label(s)

    stdout = Stdout()

    @wraps(old_print)
    def print(*args: object, **kwargs: object):
        old_print(file=stdout, *args, **kwargs)

    patched_builtins["print"] = print

    globals_dict = {"__builtins__": patched_builtins}
    try:
        exec(code, globals_dict)
    except BaseException as exc:
        print_exc(file=stdout)
        if not isinstance(exc, Exception):
            raise

    try:
        ret = globals_dict["__ret__"]
    except KeyError:
        pass
    else:
        if isawaitable(ret):
            try:
                ret = await ret
            except BaseException as exc:
                print_exc(file=stdout)
                if not isinstance(exc, Exception):
                    raise
        stdout.write(str(ret))


@ui.page("/debug")
async def debug() -> None:
    """
    Debug page.
    """
    layout("Debug")

    with ui.row():
        code_textarea = ui.textarea(label="Evaluate Python code")
        ui.button(icon="send", on_click=lambda: eval_code.refresh(code_textarea.value))
    eval_code_ret = eval_code("")
    assert isawaitable(eval_code_ret)
    await eval_code_ret
