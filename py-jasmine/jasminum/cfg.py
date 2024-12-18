import polars as pl

from .j import J


def strlen(len: J) -> J:
    pl.Config.set_fmt_str_lengths(len.int())
    return J(None)


def tbl(rows: J, columns: J) -> J:
    pl.Config.set_tbl_rows(rows.int())
    pl.Config.set_tbl_cols(columns.int())
    return J(None)
