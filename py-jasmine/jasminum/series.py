import polars as pl

from .exceptions import JasmineEvalException
from .j import J, JType


def asc(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sort())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("asc", arg.j_type.name)
        )


def bfill(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().backward_fill())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("bfill", arg.j_type.name)
        )


def count(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().count())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("count", arg.j_type.name)
        )


def ccount(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cum_count())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "ccount", arg.j_type.name
            )
        )


def desc(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sort(descending=True))
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("desc", arg.j_type.name)
        )


def first(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().first())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("first", arg.j_type.name)
        )


def flatten(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().flatten())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "flatten", arg.j_type.name
            )
        )


def ffill(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().forward_fill())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("ffill", arg.j_type.name)
        )


def hash(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().hash())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("hash", arg.j_type.name)
        )


def last(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().last())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("last", arg.j_type.name)
        )


def next(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().shift(-1))
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("next", arg.j_type.name)
        )


def isnull(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().is_null())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "isnull", arg.j_type.name
            )
        )


def prev(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().shift())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("prev", arg.j_type.name)
        )


def rank(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().rank())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("rank", arg.j_type.name)
        )


def reverse(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().reverse())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "reverse", arg.j_type.name
            )
        )


def shuffle(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().shuffle())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "shuffle", arg.j_type.name
            )
        )


def unique(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().unique())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "unique", arg.j_type.name
            )
        )


def uc(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().unique_counts())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("uc", arg.j_type.name)
        )


def bottom(k: J, arg: J) -> J:
    if k.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().bottom_k(k.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "bottom", k.j_type.name, arg.j_type.name
            )
        )


def differ(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().list.set_difference(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "differ", arg1.j_type.name, arg2.j_type.name
            )
        )


def top(k: J, arg: J) -> J:
    if k.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().top_k(k.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "top", k.j_type.name, arg.j_type.name
            )
        )


def fill(value: J, arg: J) -> J:
    if value.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().fill_null(value.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "fill", value.j_type.name, arg.j_type.name
            )
        )


def in_(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().is_in(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "in", arg1.j_type.name, arg2.j_type.name
            )
        )


def intersect(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().list.set_intersection(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "intersect", arg1.j_type.name, arg2.j_type.name
            )
        )


def shift(n: J, arg: J) -> J:
    if n.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().shift(n.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "shift", n.j_type.name, arg.j_type.name
            )
        )


def ss(arg: J, element: J) -> J:
    if arg.j_type == JType.EXPR or element.j_type == JType.EXPR:
        return J(arg.to_expr().search_sorted(element.to_expr(), side="left"))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "ss", arg.j_type.name, element.j_type.name
            )
        )


def ssr(arg: J, element: J) -> J:
    if arg.j_type == JType.EXPR or element.j_type == JType.EXPR:
        return J(arg.to_expr().search_sorted(element.to_expr(), side="right"))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "ssr", arg.j_type.name, element.j_type.name
            )
        )


def union(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().list.set_union(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "union", arg1.j_type.name, arg2.j_type.name
            )
        )
