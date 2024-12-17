from typing import Callable

from .ast import AstFn


class JFn:
    fn: Callable | AstFn | None
    args: dict
    arg_names: list[str]
    arg_num: int

    def __init__(
        self,
        fn: Callable | AstFn | None,
        args: dict,
        arg_names: list[str],
        arg_num: int,
    ) -> None:
        self.fn = fn
        self.args = args
        self.arg_names = arg_names
        self.arg_num = arg_num

    def __str__(self):
        if isinstance(self.fn, AstFn):
            return self.fn.fn_body
        else:
            return f"fn({", ".join(self.arg_names)})"

    def is_built_in(self):
        return isinstance(self.fn, Callable)
