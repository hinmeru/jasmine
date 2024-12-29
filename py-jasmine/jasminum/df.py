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
