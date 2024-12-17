from .exceptions import JasmineEvalException
from .j import J, JType


def aj(on: J, df1: J, df2: J) -> J:
    columns = on.to_strs()
    if len(columns) == 0:
        raise JasmineEvalException("requires at least one asof column for 'aj'")
    d1 = df1.to_df()
    d2 = df2.to_df()
    d = d1.join_asof(d2, on=columns[-1], by=columns[0:-1], coalesce=True)
    return J(d)
