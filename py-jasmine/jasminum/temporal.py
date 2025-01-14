import time
from datetime import date

import polars as pl

from .ast import get_timezone
from .exceptions import JasmineEvalException
from .j import J, JType


def convert_tz(datetime: J, tz: J) -> J:
    tzinfo = tz.to_str()
    datetime.assert_types([JType.DATETIME, JType.TIMESTAMP, JType.SERIES, JType.EXPR])
    if datetime.j_type == JType.DATETIME or datetime.j_type == JType.TIMESTAMP:
        return datetime.with_timezone(tzinfo)
    elif datetime.j_type == JType.SERIES:
        return J(datetime.data.dt.convert_time_zone(tzinfo))
    elif datetime.j_type == JType.EXPR:
        return J(datetime.to_expr().dt.convert_time_zone(tzinfo))


def replace_tz(datetime: J, tz: J) -> J:
    tzinfo = tz.to_str()
    datetime.assert_types([JType.DATETIME, JType.TIMESTAMP, JType.SERIES, JType.EXPR])
    if datetime.j_type == JType.DATETIME or datetime.j_type == JType.TIMESTAMP:
        num = datetime.to_series().dt.replace_time_zone(tzinfo).cast(pl.Int64).first()
        if datetime.j_type == JType.TIMESTAMP:
            return J.from_nanos(num, tzinfo)
        else:
            return J.from_millis(num, tzinfo)
    elif datetime.j_type == JType.SERIES:
        return J(datetime.data.dt.replace_time_zone(tzinfo))
    elif datetime.j_type == JType.EXPR:
        return J(datetime.to_expr().dt.replace_time_zone(tzinfo))


def offset(datetime: J) -> J:
    if datetime.j_type == JType.DATETIME or datetime.j_type == JType.TIMESTAMP:
        series = datetime.to_series()
        return J(
            (series.dt.dst_offset() + series.dt.base_utc_offset())
            .cast(pl.Int64)
            .first()
            * 1000000,
            JType.DURATION,
        )
    elif datetime.j_type == JType.SERIES:
        return J(
            (datetime.data.dt.dst_offset() + datetime.data.dt.base_utc_offset()).cast(
                pl.Duration("ns")
            )
        )
    elif datetime.j_type == JType.EXPR:
        return (
            datetime.to_expr().dt.dst_offset() + datetime.to_expr().dt.base_utc_offset()
        ).cast(pl.Duration("ns"))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}'".format(
                "offset", datetime.j_type.name
            )
        )


def utcoffset(datetime: J) -> J:
    if datetime.j_type == JType.DATETIME or datetime.j_type == JType.TIMESTAMP:
        series = datetime.to_series()
        return J(
            series.dt.base_utc_offset().cast(pl.Int64).first() * 1000000,
            JType.DURATION,
        )
    elif datetime.j_type == JType.SERIES:
        return J(datetime.data.dt.base_utc_offset().cast(pl.Duration("ns")))
    elif datetime.j_type == JType.EXPR:
        return datetime.to_expr().dt.base_utc_offset().cast(pl.Duration("ns"))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}'".format(
                "utcoffset", datetime.j_type.name
            )
        )


def dstoffset(datetime: J) -> J:
    if datetime.j_type == JType.DATETIME or datetime.j_type == JType.TIMESTAMP:
        series = datetime.to_series()
        return J(
            series.dt.dst_offset().cast(pl.Int64).first() * 1000000,
            JType.DURATION,
        )
    elif datetime.j_type == JType.SERIES:
        return J(datetime.data.dt.dst_offset().cast(pl.Duration("ns")))
    elif datetime.j_type == JType.EXPR:
        return datetime.to_expr().dt.dst_offset().cast(pl.Duration("ns"))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}'".format(
                "dstoffset", datetime.j_type.name
            )
        )


def now() -> J:
    tz = get_timezone()
    # utc nanos
    ns = int(time.time() * 1000000000)
    return J.from_nanos(ns, tz)


def today() -> J:
    return J(date.today())


def combine(date: J, time: J) -> J:
    if date.j_type == JType.EXPR or time.j_type == JType.EXPR:
        return J(
            date.to_expr()
            .dt.combine(time.to_expr())
            .dt.replace_time_zone(get_timezone())
        )
    elif date.j_type == JType.SERIES or time.j_type == JType.SERIES:
        return J(date.data.dt.combine(time.data).dt.replace_time_zone(get_timezone()))
    elif date.j_type == JType.DATE or time.j_type == JType.TIME:
        num = (
            J.from_nanos(date.nanos_from_epoch() + time.data, "UTC")
            .to_series()
            .dt.replace_time_zone(get_timezone())
            .cast(pl.Int64)
            .first()
        )
        return J.from_nanos(num, get_timezone())
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}'".format(
                "combine", date.j_type.name
            )
        )
