import polars as pl
import polars.selectors as cs

from .exceptions import JasmineEvalException
from .j import J, JType


def selector(column: J) -> J:
    return J(cs.matches(column.to_str()))


def col(column: J) -> J:
    return J(pl.col(column.to_str()))


def lit(value: J) -> J:
    try:
        return J(value.to_expr())
    except Exception:
        raise JasmineEvalException(f"failed to apply 'lit': {value}")
