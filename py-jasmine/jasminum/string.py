import polars as pl

from .exceptions import JasmineEvalException
from .j import J, JType


def lowercase(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().str.to_lowercase())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "lowercase", arg.j_type.name
            )
        )


def strips(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().str.strip_chars_start())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "strips", arg.j_type.name
            )
        )


def stripe(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().str.strip_chars_end())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "stripe", arg.j_type.name
            )
        )


def strip(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().str.strip_chars())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("strip", arg.j_type.name)
        )


def string(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cast(pl.String))
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "string", arg.j_type.name
            )
        )


def uppercase(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().str.to_uppercase())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "uppercase", arg.j_type.name
            )
        )


def like(strings: J, pattern: J) -> J:
    if strings.j_type == JType.EXPR or pattern.j_type == JType.EXPR:
        return J(strings.to_expr().str.contains(pattern.to_expr(), strict=True))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "like", strings.j_type.name, pattern.j_type.name
            )
        )


def matches(strings: J, pattern: J) -> J:
    if strings.j_type == JType.EXPR or pattern.j_type == JType.EXPR:
        return J(strings.to_expr().str.count_matches(pattern.to_expr(), strict=True))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "matches", strings.j_type.name, pattern.j_type.name
            )
        )


def join(separator: J, strings: J) -> J:
    if separator.j_type == JType.EXPR or strings.j_type == JType.EXPR:
        return J(strings.to_expr().list.join(separator.to_expr(), ignore_nulls=True))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "join", separator.j_type.name, strings.j_type.name
            )
        )


def split(separator: J, string: J) -> J:
    if separator.j_type == JType.EXPR or string.j_type == JType.EXPR:
        return J(string.to_expr().str.split(separator.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "split", separator.j_type.name, string.j_type.name
            )
        )


def replace(original: J, pattern: J, value: J) -> J:
    if (
        original.j_type == JType.EXPR
        or pattern.j_type == JType.EXPR
        or value.j_type == JType.EXPR
    ):
        return J(original.to_expr().str.replace(pattern.to_expr(), value.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}', '{2}' and '{3}'".format(
                "replace", original.j_type.name, pattern.j_type.name, value.j_type.name
            )
        )


def extract(string: J, pattern: J) -> J:
    if string.j_type == JType.EXPR or pattern.j_type == JType.EXPR:
        return J(string.to_expr().str.extract(pattern.to_expr(), group_index=0))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "extract", string.j_type.name, pattern.j_type.name
            )
        )


def parse_date(string: J, pattern: J) -> J:
    if string.j_type == JType.EXPR:
        return J(string.to_expr().str.to_date(pattern.to_str()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "extract", string.j_type.name, pattern.j_type.name
            )
        )


def parse_datetime(string: J, pattern: J) -> J:
    if string.j_type == JType.EXPR:
        return J(string.to_expr().str.to_datetime(pattern.to_str()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "extract", string.j_type.name, pattern.j_type.name
            )
        )
