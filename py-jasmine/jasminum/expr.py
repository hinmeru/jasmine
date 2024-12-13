import polars as pl
import polars.selectors as cs

from .j import J


def selector(column: J) -> J:
    return J(cs.matches(column.to_str()))


def col(column: J) -> J:
    return J(pl.col(column.to_str()))
