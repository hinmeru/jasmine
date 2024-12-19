import polars as pl

from .constant import PL_DTYPE_TO_J_TYPE
from .exceptions import JasmineEvalException
from .j import J


def aj(on: J, df1: J, df2: J) -> J:
    columns = on.to_strs()
    if len(columns) == 0:
        raise JasmineEvalException("requires at least one asof column for 'aj'")
    d1 = df1.to_df()
    d2 = df2.to_df()
    d = d1.join_asof(d2, on=columns[-1], by=columns[0:-1], coalesce=True)
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
