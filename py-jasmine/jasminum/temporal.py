import time
from datetime import date

import polars as pl

from .ast import get_timezone
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


def now() -> J:
    tz = get_timezone()
    # utc nanos
    ns = int(time.time() * 1000000000)
    return J.from_nanos(ns, tz)


def today() -> J:
    return J(date.today())
