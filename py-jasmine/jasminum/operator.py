from datetime import timedelta
from typing import Callable

import numpy as np
import polars as pl

from .constant import PL_DATA_TYPE
from .exceptions import JasmineEvalException
from .j import J, JType


def list_op_list(arg1: J, arg2: J, fn: Callable) -> J:
    if len(arg1) != len(arg2):
        raise JasmineEvalException(
            "length error '{0}' vs '{1}'".format(len(arg1), len(arg2))
        )
    else:
        res = []
        for j1, j2 in zip(arg1.data, arg2.data):
            res.append(fn(j1, j2))
        return J(res)


def dict_op_list(arg1: J, arg2: J, fn: Callable) -> J:
    if len(arg1) != len(arg2):
        raise JasmineEvalException(
            "length error '{0}' vs '{1}'".format(len(arg1), len(arg2))
        )
    else:
        new_dict = arg1.data.copy()
        for i, k in enumerate(arg1.data):
            new_dict[k] = fn(new_dict[k], arg2.data[i])
        return J(new_dict)


def list_op_dict(arg1: J, arg2: J, fn: Callable) -> J:
    if len(arg1) != len(arg2):
        raise JasmineEvalException(
            "length error '{0}' vs '{1}'".format(len(arg1), len(arg2))
        )
    else:
        new_dict = arg2.data.copy()
        for i, k in enumerate(arg2.data):
            new_dict[k] = fn(arg1.data[i], new_dict[k])
        return J(new_dict)


def list_op_scalar(arg1: J, arg2: J, fn: Callable) -> J:
    res = []
    for j1 in arg1.data:
        res.append(fn(j1, arg2))
    return J(res)


def scalar_op_list(arg1: J, arg2: J, fn: Callable) -> J:
    res = []
    for j2 in arg2.data:
        res.append(fn(arg1, j2))
    return J(res)


def dict_op_scalar(arg1: J, arg2: J, fn: Callable) -> J:
    new_dict = arg1.data.copy()
    for k, v in arg1.data.items():
        new_dict[k] = fn(v, arg2)
    return J(new_dict)


def scalar_op_dict(arg1: J, arg2: J, fn: Callable) -> J:
    new_dict = arg2.data.copy()
    for k, v in arg2.data.items():
        new_dict[k] = fn(arg1, v)
    return J(new_dict)


# |           | date | time | datetime | timestamp | duration  |
# | --------- | ---- | ---- | -------- | --------- | --------- |
# | date      | -    | -    | -        | -         | date      |
# | time      | -    | -    | -        | -         | -         |
# | datetime  | -    | -    | -        | -         | duration  |
# | timestamp | -    | -    | -        | -         | timestamp |
# | duration  | date | -    | datetime | timestamp | duration  |
def add(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().add(arg2.to_expr()))
    elif arg1.j_type == JType.NULL or arg2.j_type == JType.NULL:
        return J(None, JType.NULL)
    elif arg1.j_type.value <= 2 and arg2.j_type.value <= 2:
        return J(arg1.data + arg2.data, JType.INT)
    elif (
        (arg1.j_type == JType.FLOAT or arg2.j_type == JType.FLOAT)
        and arg1.is_numeric_scalar()
        and arg2.is_numeric_scalar()
    ):
        return J(arg1.data + arg2.data, JType.FLOAT)
    elif arg1.j_type == JType.DATE and arg2.j_type == JType.DURATION:
        return J(arg1.data + timedelta(days=arg2.days()))
    elif arg1.j_type == JType.TIMESTAMP and arg2.j_type == JType.DURATION:
        return J.from_nanos(arg1.nanos_from_epoch() + arg2.data, arg1.tz())
    elif arg1.j_type == JType.DATETIME and arg2.j_type == JType.DURATION:
        return J.from_millis(arg1.data + arg2.data // 1000000, arg1.tz())
    elif arg1.j_type == JType.DURATION and arg2.j_type == JType.DURATION:
        return J(arg1.data + arg2.data, JType.DURATION)
    elif (
        arg1.j_type == JType.STRING or arg1.j_type == JType.CAT
    ) and arg2.j_type.value <= 11:
        return J(arg1.data + str(arg2), arg1.j_type)
    elif (
        arg2.j_type == JType.STRING
        or arg2.j_type == JType.CAT
        and arg1.j_type.value <= 11
    ):
        return J(str(arg1) + arg2.data, arg2.j_type)
    elif arg1.j_type == JType.SERIES and arg2.j_type.value <= 11:
        if arg2.is_temporal_scalar():
            return J(arg1.data + arg2.to_series())
        else:
            return J(arg1.data + arg2.data)
    elif arg1.j_type == JType.LIST and arg2.j_type.value <= 10:
        return list_op_scalar(arg1, arg2, add)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.LIST:
        return list_op_list(arg1, arg2, add)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.LIST:
        return scalar_op_list(arg1, arg2, add)
    elif arg1.j_type == JType.DICT and arg2.j_type.value <= 10:
        return dict_op_scalar(arg1, arg2, add)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.DICT:
        new_dict = {}
        for k, v in arg1.data.items():
            if k in arg2.data:
                new_dict[k] = add(v, arg2.data[k])
            else:
                new_dict[k] = v
        for k, v in arg2.data.items():
            if k not in arg1.data:
                new_dict[k] = v
        return J(new_dict)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.DICT:
        return scalar_op_dict(arg1, arg2, add)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.DICT:
        return list_op_dict(arg1, arg2, add)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.LIST:
        return dict_op_list(arg1, arg2, add)
    elif (
        arg1.j_type == JType.DURATION and arg2.j_type.value >= 3 and arg2.j_type <= 6
    ) or (arg1.j_type.value <= 10 and arg2.j_type == JType.SERIES):
        return add(arg2, arg1)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "add", arg1.j_type.name, arg2.j_type.name
            )
        )


def sub(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().sub(arg2.to_expr()))
    elif arg1.j_type == JType.NULL or arg2.j_type == JType.NULL:
        return J(None, JType.NULL)
    elif arg1.j_type.value <= 2 and arg2.j_type.value <= 2:
        return J(arg1.data - arg2.data, JType.INT)
    elif (
        (arg1.j_type == JType.FLOAT or arg2.j_type == JType.FLOAT)
        and arg1.is_numeric_scalar()
        and arg2.is_numeric_scalar()
    ):
        return J(arg1.data - arg2.data, JType.FLOAT)
    elif arg1.j_type == JType.DATE and arg2.j_type == JType.DURATION:
        return J(arg1.data - timedelta(days=arg2.days()))
    elif arg1.j_type == JType.TIMESTAMP and arg2.j_type == JType.DURATION:
        return J.from_nanos(arg1.nanos_from_epoch() - arg2.data, arg1.tz())
    elif arg1.j_type == JType.DATETIME and arg2.j_type == JType.DURATION:
        return J.from_millis(arg1.data - arg2.data // 1000000, arg1.tz())
    elif arg1.j_type == JType.DURATION and arg2.j_type == JType.DURATION:
        return J(arg1.data - arg2.data, JType.DURATION)
    elif (arg1.j_type == JType.SERIES and arg2.j_type.value <= 11) or (
        arg2.j_type == JType.SERIES and arg1.j_type.value <= 11
    ):
        if arg2.is_temporal_scalar():
            return J(arg1.data - arg2.to_series())
        else:
            return J(arg1.data - arg2.data)
    elif arg1.j_type == JType.LIST and arg2.j_type.value <= 10:
        return list_op_scalar(arg1, arg2, sub)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.LIST:
        return list_op_list(arg1, arg2, sub)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.LIST:
        return scalar_op_list(arg1, arg2, sub)
    elif arg1.j_type == JType.DICT and arg2.j_type.value <= 10:
        return dict_op_scalar(arg1, arg2, sub)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.DICT:
        new_dict = {}
        for k, v in arg1.data.items():
            if k in arg2.data:
                new_dict[k] = sub(v, arg2.data[k])
            else:
                new_dict[k] = v
        for k, v in arg2.data.items():
            if k not in arg1.data:
                new_dict[k] = v.neg()
        return J(new_dict)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.DICT:
        return scalar_op_dict(arg1, arg2, sub)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.DICT:
        return list_op_dict(arg1, arg2, sub)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.LIST:
        return dict_op_list(arg1, arg2, sub)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "-", arg1.j_type.name, arg2.j_type.name
            )
        )


def pow(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().pow(arg2.to_expr()))
    elif arg1.j_type == JType.NULL or arg2.j_type == JType.NULL:
        return J(None, JType.NULL)
    elif (
        (arg1.j_type.value <= 2 and arg2.j_type.value <= 2)
        or (
            (arg1.j_type == JType.FLOAT or arg2.j_type == JType.FLOAT)
            and arg1.is_numeric_scalar()
            and arg2.is_numeric_scalar()
        )
        or (arg1.j_type == JType.SERIES and arg2.is_numeric_scalar())
        or (arg1.j_type == JType.SERIES and arg2.j_type == JType.SERIES)
        or (arg1.is_numeric_scalar() and arg2.j_type == JType.SERIES)
    ):
        return J(arg1.data**arg2.data)
    elif arg1.j_type == JType.LIST and arg2.j_type.value <= 10:
        return list_op_scalar(arg1, arg2, pow)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.LIST:
        return list_op_list(arg1, arg2, pow)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.LIST:
        return scalar_op_list(arg1, arg2, pow)
    elif arg1.j_type == JType.DICT and arg2.j_type.value <= 10:
        return dict_op_scalar(arg1, arg2, pow)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.DICT:
        new_dict = {}
        for k, v in arg1.data.items():
            if k in arg2.data:
                new_dict[k] = pow(v, arg2.data[k])
            else:
                new_dict[k] = v
        for k, v in arg2.data.items():
            if k not in arg1.data:
                new_dict[k] = v
        return J(new_dict)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.DICT:
        return scalar_op_dict(arg1, arg2, pow)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.DICT:
        return list_op_dict(arg1, arg2, pow)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.LIST:
        return dict_op_list(arg1, arg2, pow)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "**", arg1.j_type.name, arg2.j_type.name
            )
        )


def mul(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().mul(arg2.to_expr()))
    elif arg1.j_type == JType.NULL or arg2.j_type == JType.NULL:
        return J(None, JType.NULL)
    elif arg1.j_type.value <= 2 and arg2.j_type.value <= 2:
        return J(arg1.data * arg2.data, JType.INT)
    elif (
        (arg1.j_type == JType.FLOAT or arg2.j_type == JType.FLOAT)
        and arg1.is_numeric_scalar()
        and arg2.is_numeric_scalar()
    ):
        return J(arg1.data * arg2.data, JType.FLOAT)
    elif (
        (
            arg1.j_type == JType.SERIES
            and arg2.j_type.value <= 11
            and not arg2.is_temporal_scalar()
        )
        or (arg1.j_type.value <= 10 and not arg1.is_temporal_scalar())
        and arg2.j_type == JType.SERIES
    ):
        return J(arg1.data * arg2.data)
    elif arg1.j_type == JType.LIST and arg2.j_type.value <= 10:
        return list_op_scalar(arg1, arg2, mul)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.LIST:
        return list_op_list(arg1, arg2, mul)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.LIST:
        return scalar_op_list(arg1, arg2, mul)
    elif arg1.j_type == JType.DICT and arg2.j_type.value <= 10:
        return dict_op_scalar(arg1, arg2, mul)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.DICT:
        new_dict = {}
        for k, v in arg1.data.items():
            if k in arg2.data:
                new_dict[k] = mul(v, arg2.data[k])
            else:
                new_dict[k] = v
        for k, v in arg2.data.items():
            if k not in arg1.data:
                new_dict[k] = v
        return J(new_dict)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.DICT:
        return scalar_op_dict(arg1, arg2, mul)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.DICT:
        return list_op_dict(arg1, arg2, mul)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.LIST:
        return dict_op_list(arg1, arg2, mul)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "*", arg1.j_type.name, arg2.j_type.name
            )
        )


def true_div(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().truediv(arg2.to_expr()))
    elif arg1.is_numeric_scalar() or arg2.is_numeric_scalar():
        return J(arg1.data / arg2.data, JType.FLOAT)
    elif (
        arg1.j_type == JType.SERIES
        and arg2.j_type.value <= 11
        and not arg2.is_temporal_scalar()
    ) or (arg2.j_type == JType.SERIES and arg1.j_type.value <= 11):
        return J(arg1.data / arg2.data)
    elif arg1.j_type == JType.LIST and arg2.j_type.value <= 10:
        return list_op_scalar(arg1, arg2, true_div)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.LIST:
        return list_op_list(arg1, arg2, true_div)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.LIST:
        return scalar_op_list(arg1, arg2, true_div)
    elif arg1.j_type == JType.DICT and arg2.j_type.value <= 10:
        return dict_op_scalar(arg1, arg2, true_div)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.DICT:
        new_dict = {}
        for k, v in arg1.data.items():
            if k in arg2.data:
                new_dict[k] = true_div(v, arg2.data[k])
            else:
                new_dict[k] = v
        for k, v in arg2.data.items():
            if k not in arg1.data:
                new_dict[k] = true_div(J(1), v)
        return J(new_dict)
    elif arg1.j_type.value <= 10 and arg2.j_type == JType.DICT:
        return scalar_op_dict(arg1, arg2, true_div)
    elif arg1.j_type == JType.LIST and arg2.j_type == JType.DICT:
        return list_op_dict(arg1, arg2, true_div)
    elif arg1.j_type == JType.DICT and arg2.j_type == JType.LIST:
        return dict_op_list(arg1, arg2, true_div)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "/", arg1.j_type.name, arg2.j_type.name
            )
        )


def mod(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().mod(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "%", arg1.j_type.name, arg2.j_type.name
            )
        )


def bin_min(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().clip(upper_bound=arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "&", arg1.j_type.name, arg2.j_type.name
            )
        )


def bin_max(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().clip(lower_bound=arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "|", arg1.j_type.name, arg2.j_type.name
            )
        )


def rand(size: J, base: J) -> J:
    if size.j_type == JType.INT:
        if base.j_type == JType.INT:
            return J(pl.Series("", np.random.randint(base.data, size=size.data)))
        elif base.j_type == JType.FLOAT:
            return J(pl.Series("", base.data * np.random.rand(size.data)))
        elif base.j_type == JType.SERIES:
            return J(base.data.sample(abs(size.data), with_replacement=size.data > 0))
        elif base.j_type == JType.DATAFRAME:
            return J(base.data.sample(abs(size.data), with_replacement=size.data > 0))
    else:
        raise JasmineEvalException(
            "'rand' requires 'int' and 'int|float', got '%s' and '%s'"
            % (size.j_type, size.j_type)
        )


def cast(type_name: J, arg: J) -> J:
    name = type_name.to_str()
    if name not in PL_DATA_TYPE and name not in [
        "year",
        "month",
        "month_start",
        "month_end",
        "weekday",
        "day",
        "dt",
        "hour",
        "minute",
        "second",
        "ms",
        "ns",
    ]:
        raise JasmineEvalException("unknown data type for 'cast': %s" % name)
    if arg.j_type == JType.EXPR:
        match name:
            case name if name in PL_DATA_TYPE:
                return J(arg.data.cast(PL_DATA_TYPE[name]))
            case "year":
                return J(arg.data.dt.year())
            case "month":
                return J(arg.data.dt.month())
            case "month_start":
                return J(arg.data.dt.month_start())
            case "month_end":
                return J(arg.data.dt.month_end())
            case "weekday":
                return J(arg.data.dt.weekday())
            case "day":
                return J(arg.data.dt.day())
            case "dt":
                return J(arg.data.dt.date())
            case "hour":
                return J(arg.data.dt.hour())
            case "minute":
                return J(arg.data.dt.minute())
            case "second":
                return J(arg.data.dt.second())
            case "t":
                return J(arg.data.dt.time())
            case "ms":
                return J(arg.data.dt.millisecond())
            case "ns":
                return J(arg.data.dt.nanosecond())
    elif arg.j_type == JType.SERIES:
        if name in PL_DATA_TYPE:
            return J(arg.data.cast(PL_DATA_TYPE[name]))
        else:
            match name:
                case "year":
                    return J(arg.data.dt.year())
                case "month":
                    return J(arg.data.dt.month())
                case "month_start":
                    return J(arg.data.dt.month_start())
                case "month_end":
                    return J(arg.data.dt.month_end())
                case "weekday":
                    return J(arg.data.dt.weekday())
                case "day":
                    return J(arg.data.dt.day())
                case "dt":
                    return J(arg.data.dt.date())
                case "hour":
                    return J(arg.data.dt.hour())
                case "minute":
                    return J(arg.data.dt.minute())
                case "second":
                    return J(arg.data.dt.second())
                case "t":
                    return J(arg.data.dt.time())
                case "ms":
                    return J(arg.data.dt.millisecond())
                case "ns":
                    return J(arg.data.dt.nanosecond())
    elif arg.j_type == JType.LIST and len(arg.data) == 0 and name in PL_DATA_TYPE:
        return J(pl.Series("", [], PL_DATA_TYPE[name]))
    elif arg.j_type.value <= 10:
        if name == "string" and arg.j_type.value <= 8:
            return J(str(arg))
        elif name == "i64":
            return J(arg.int())
        elif name == "f64":
            return J(arg.float())

        match arg.j_type:
            case JType.STRING:
                if name == "cat":
                    return J(arg.data, JType.CAT)
                elif name == "string":
                    return arg
            case JType.CAT:
                if name == "string":
                    return J(arg.data, JType.STRING)
                elif name == "cat":
                    return arg

        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "cast", type_name.j_type.name, arg.j_type.name
            )
        )
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "cast", type_name.j_type.name, arg.j_type.name
            )
        )


def not_equal(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().ne_missing(arg2.to_expr()))
    elif arg1.j_type == JType.NULL and arg2.j_type == JType.NULL:
        return J(True)
    elif arg1.j_type == JType.NULL or arg2.j_type == JType.NULL:
        return J(False)
    elif arg1.is_numeric_scalar() and arg2.is_numeric_scalar():
        return J(arg1.data != arg2.data)
    elif arg1.j_type == JType.STRING and arg2.j_type == JType.STRING:
        return J(arg1.data != arg2.data)
    elif arg1.j_type == JType.CAT and arg2.j_type == JType.CAT:
        return J(arg1.data != arg2.data)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "!=", arg1.j_type.name, arg2.j_type.name
            )
        )


def less_equal(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().le(arg2.to_expr()))
    elif arg1.is_numeric_scalar() and arg2.is_numeric_scalar():
        return J(arg1.data <= arg2.data)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "<=", arg1.j_type.name, arg2.j_type.name
            )
        )


def great_equal(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().ge(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                ">=", arg1.j_type.name, arg2.j_type.name
            )
        )


def less_than(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().lt(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "<", arg1.j_type.name, arg2.j_type.name
            )
        )


def great_than(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().gt(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                ">", arg1.j_type.name, arg2.j_type.name
            )
        )


def equal(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().eq(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "==", arg1.j_type.name, arg2.j_type.name
            )
        )


def get(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().get(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "@", arg1.j_type.name, arg2.j_type.name
            )
        )


def concat_list(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(pl.concat_list([arg1.to_expr(), arg2.to_expr()]))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "++", arg1.j_type.name, arg2.j_type.name
            )
        )


def take(n: J, arg: J) -> J:
    if arg.j_type == JType.EXPR:
        num = n.int()
        if num >= 0:
            return J(pl.Expr.head(arg.to_expr(), num))
        else:
            return J(pl.Expr.tail(arg.to_expr(), abs(num)))
    if n.j_type == JType.INT and arg.j_type == JType.SERIES:
        num = n.int()
        if num == 0:
            return J(arg.data.head(0))
        else:
            s = arg.data
            if s.is_empty():
                s.extend(pl.Series("", [None] * s.len()))
            while len(s) < abs(num):
                s.extend(s)
            if num > 0:
                return J(s.head(num))
            else:
                return J(s.tail(abs(num)))
    elif n.j_type == JType.INT and arg.j_type == JType.DATAFRAME:
        num = n.int()
        if num == 0:
            return J(arg.data.head(0))
        else:
            df = arg.data
            if df.is_empty():
                df = pl.DataFrame(
                    {
                        col: pl.Series(col, [None] * num, dtype=dt)
                        for col, dt in zip(df.columns, df.dtypes)
                    }
                )
            while len(df) < abs(num):
                df = pl.concat([df, df])
            if num > 0:
                return J(df.head(num))
            else:
                return J(df.tail(abs(num)))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "#", n.j_type.name, arg.j_type.name
            )
        )


def xor(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().xor(arg2.to_expr()))
    elif arg1.j_type == JType.BOOL and arg2.j_type == JType.BOOL:
        return J(arg1.data ^ arg2.data, JType.BOOL)
    elif arg1.j_type.value <= 2 and arg2.j_type.value <= 2:
        return J(arg1.data ^ arg2.data, JType.INT)
    elif (
        (arg1.j_type == JType.SERIES and arg2.is_numeric_scalar())
        or (arg1.is_numeric_scalar() and arg2.j_type == JType.SERIES)
        or (arg1.j_type == JType.SERIES and arg2.j_type == JType.SERIES)
    ):
        return J(arg1.data ^ arg2.data)
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "^", arg1.j_type.name, arg2.j_type.name
            )
        )


def range(start: J, end: J) -> J:
    if start.j_type == JType.EXPR or end.j_type == JType.EXPR:
        return J(pl.arange(start.to_expr(), end.to_expr()))
    elif start.j_type == JType.INT and end.j_type == JType.INT:
        return J(pl.arange(start.int(), end.int(), eager=True))
    elif start.j_type == JType.DATE and end.j_type == JType.DATE:
        return J(pl.date_range(start.data, end.data, eager=True))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "..", start.j_type.name, end.j_type.name
            )
        )
