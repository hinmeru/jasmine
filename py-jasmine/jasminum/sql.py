import polars as pl

from .exceptions import JasmineEvalException
from .j import J, JType


def is_between(expr: J, bounds: J):
    if bounds.j_type == JType.LIST and len(bounds.data) == 2:
        return J(pl.Expr.is_between(expr.to_expr(), *bounds.to_exprs()))

    elif bounds.j_type == JType.SERIES and bounds.data.count() == 2:
        return J(
            pl.Expr.is_between(
                expr.to_expr(), pl.lit(bounds.data[0]), pl.lit(bounds.data[1])
            )
        )
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "%", expr.j_type.name, bounds.j_type.name
            )
        )


def over(expr: J, groups: J):
    return J(expr.to_expr().over(groups.to_exprs()))
