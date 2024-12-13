from datetime import date

import polars as pl
import pytest

from jasminum.context import Context
from jasminum.engine import Engine
from jasminum.eval import eval_src
from jasminum.j import J, JType


@pytest.mark.parametrize(
    "src,expect",
    [
        ("1+1", J(2, JType.INT)),
        ("2024-10-23+1D00:12:34.5", J(date(2024, 10, 24))),
        ("2024-10-23D+0D00:12:34.5", J.from_nanos(1729642354500000000, "UTC")),
        ("[,,]", J(pl.Series("", [None, None, None]))),
        (
            "df[series1 = [0i32,,], series2 = [none, 2.0, 3.0]]",
            J(
                pl.DataFrame(
                    [
                        pl.Series("series1", [0, None, None], pl.Int32),
                        pl.Series("series2", [None, 2.0, 3.0], pl.Float64),
                    ]
                )
            ),
        ),
    ],
)
def test_simple_src(src, expect):
    engine = Engine()
    res = eval_src(src, 0, engine, Context(dict()))
    if res.j_type in (JType.DATAFRAME, JType.SERIES):
        res.data.equals(expect.data)
    else:
        assert res == expect
