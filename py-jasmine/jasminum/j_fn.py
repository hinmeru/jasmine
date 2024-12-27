from typing import Callable

from .ast import AstFn


class JFn:
    fn: Callable | AstFn | None | str
    args: dict
    arg_names: list[str]
    arg_num: int
    name: str

    def __init__(
        self,
        fn: Callable | AstFn | None,
        args: dict,
        arg_names: list[str],
        arg_num: int,
        name="",
    ) -> None:
        self.fn = fn
        self.args = args
        self.arg_names = arg_names
        self.arg_num = arg_num
        self.name = name

    def __str__(self):
        if isinstance(self.fn, AstFn):
            return self.fn.fn_body
        else:
            return f"fn({", ".join(self.arg_names)}){{}}"

    def __repr__(self):
        return self.name if self.name else self.fn.fn_body

    def is_built_in(self):
        return isinstance(self.fn, Callable)

    def get_statements(self):
        if isinstance(self.fn, AstFn):
            return self.fn.stmts
        else:
            return []
