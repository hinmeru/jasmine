import polars as pl

from .constant import PL_DTYPE_TO_J_TYPE
from .exceptions import JasmineEvalException
from .j import J, JType
from .util import validate_args


def aj(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    columns = on.to_strs()
    if len(columns) == 0:
        raise JasmineEvalException("requires at least one asof column for 'aj'")
    d1 = df1.to_df()
    d2 = df2.to_df()
    d = d1.join_asof(d2, on=columns[-1], by=columns[0:-1], coalesce=True)
    return J(d)


def ij(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="inner", coalesce=True)
    return J(d)


def lj(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="left", coalesce=True)
    return J(d)


# full join
def fj(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="full", coalesce=True)
    return J(d)


def oj(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="outer", coalesce=True)
    return J(d)


def cj(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="cross", coalesce=True)
    return J(d)


def semi(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="semi", coalesce=True)
    return J(d)


def anti(on: J, df1: J, df2: J) -> J:
    validate_args(
        [on, df1, df2],
        [[JType.STRING, JType.CAT, JType.SERIES], JType.DATAFRAME, JType.DATAFRAME],
    )
    d = df1.to_df().join(df2.to_df(), on=on.to_strs(), how="anti", coalesce=True)
    return J(d)


# inplace extend
def extend(df: J, other: J) -> J:
    validate_args([df, other], [JType.DATAFRAME, JType.DATAFRAME])
    d = df.to_df().extend(other.to_df())
    return J(d)


def vstack(df: J, other: J) -> J:
    validate_args([df, other], [JType.DATAFRAME, JType.DATAFRAME])
    d = df.to_df().vstack(other.to_df())
    return J(d)


def hstack(df: J, other: J) -> J:
    validate_args([df, other], [JType.DATAFRAME, JType.DATAFRAME])
    d = df.to_df().hstack(other.to_df())
    return J(d)


def polars_dtype_to_j_type(dtype: pl.DataType) -> J:
    if isinstance(dtype, pl.Datetime):
        if dtype.time_unit == "ns":
            return J("timestamp")
        elif dtype.time_unit == "ms":
            return J("datetime")
        else:
            return J("datetime(us)")
    else:
        return J(PL_DTYPE_TO_J_TYPE.get(dtype, "unknown"))


def schema(df: J) -> J:
    dataframe = df.to_df()
    s = dataframe.schema
    schema_dict = {k: polars_dtype_to_j_type(v) for k, v in s.items()}
    return J(schema_dict)


def glimpse(df: J) -> J:
    return J(df.to_df().glimpse(max_items_per_column=10))


def describe(df: J) -> J:
    return J(df.to_df().describe())


def rechunk(df: J) -> J:
    return J(df.to_df().rechunk())


def rename(columns: J, df: J) -> J:
    validate_args(
        [columns, df],
        [[JType.STRING, JType.CAT, JType.SERIES, JType.DICT], JType.DATAFRAME],
    )
    data = df.to_df()
    rename_dict = {}
    if (
        columns.j_type == JType.SERIES
        or columns.j_type == JType.CAT
        or columns.j_type == JType.STRING
    ):
        new_columns = columns.to_strs()
        for i, col in enumerate(new_columns):
            rename_dict[data.columns[i]] = col
    elif columns.j_type == JType.DICT:
        rename_dict = {k: v.to_str() for k, v in columns.data.items()}
    else:
        raise JasmineEvalException(
            "expect 'STRING|CAT|STRINGS|CATS', but got %s" % columns.j_type.name
        )
    return J(data.rename(rename_dict))


def sel(df: J, wheres: J, groups: J, columns: J) -> J:
    validate_args(
        [df, wheres, groups, columns],
        [
            JType.DATAFRAME,
            [JType.LIST, JType.EXPR, JType.NULL],
            [JType.LIST, JType.EXPR, JType.NULL],
            [JType.LIST, JType.EXPR, JType.NULL],
        ],
    )
    data = df.to_df().lazy()
    if wheres.j_type != JType.NULL:
        data = data.filter(wheres.to_exprs())

    if groups.j_type != JType.NULL and columns.j_type != JType.NULL:
        data = data.group_by(groups.to_exprs()).agg(columns.to_exprs())
    elif groups.j_type != JType.NULL and columns.j_type == JType.NULL:
        data = data.group_by(groups.to_exprs()).agg(pl.all().last())
    elif groups.j_type == JType.NULL and columns.j_type != JType.NULL:
        data = data.select(columns.to_exprs())
    else:
        return J(df)

    return J(data.collect())


def del_(df: J, wheres: J, columns: J) -> J:
    validate_args(
        [df, wheres, columns],
        [
            JType.DATAFRAME,
            [JType.LIST, JType.EXPR, JType.NULL],
            [JType.LIST, JType.EXPR, JType.NULL],
        ],
    )
    data = df.to_df().lazy()

    if wheres.j_type != JType.NULL and columns.j_type != JType.NULL:
        raise JasmineEvalException(
            "not supported 'where' and 'columns' at the same time for 'delete'"
        )

    if wheres.j_type != JType.NULL:
        for expr in wheres.to_exprs():
            data = data.filter(~expr)

    if columns.j_type != JType.NULL:
        data = data.drop(columns.to_exprs())

    return J(data.collect())


def upd(df: J, wheres: J, groups: J, columns: J) -> J:
    validate_args(
        [df, wheres, groups, columns],
        [
            JType.DATAFRAME,
            [JType.LIST, JType.EXPR, JType.NULL],
            [JType.LIST, JType.EXPR, JType.NULL],
            [JType.LIST, JType.EXPR, JType.NULL],
        ],
    )
    data = df.to_df().lazy()

    if columns.j_type == JType.NULL:
        raise JasmineEvalException(
            "requires at least one column operation for 'update'"
        )

    if wheres.j_type != JType.NULL:
        data = data.filter(wheres.to_exprs())

    if groups.j_type != JType.NULL:
        group_exprs = groups.to_exprs()
        for expr in columns.to_exprs():
            data = data.with_columns(expr.over(group_exprs))
    else:
        data = data.with_columns(columns.to_exprs())

    return J(data.collect())
