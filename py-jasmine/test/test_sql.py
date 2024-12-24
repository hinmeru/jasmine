import pytest

from jasminum.context import Context
from jasminum.engine import Engine
from jasminum.eval import eval_src
from jasminum.j import JType


@pytest.fixture
def prepare_engine() -> Engine:
    engine = Engine()
    src = """
    trade = df[
        time=2024-12-18D+`duration$150*0 .. 9,
        sym=9#`a`b`c,
        qty=100*1 .. 10,
        price=0.1 * 1 .. 10,
    ];
    quote = df[
        time=2024-12-18D+`duration$100*0 .. 15,
        sym=15#`a`b`c,
        ask=-0.05+0.1 * 1 .. 16,
        ask_size=100* 1 .. 16,
        bid=0.05+0.1 * 1 .. 16,
        bid_size=100* 1 .. 16,
    ];
    """
    eval_src(src, 0, engine, Context(dict()))
    return engine


@pytest.mark.parametrize(
    "src,expect",
    [
        ("select count i dyn 1000ns, time, sym from trade", (5, 3)),
        ("select count i dyn 1000ns, time, sym from quote", (6, 3)),
        ("select from trade where price ~between [0.3, 0.5]", (3, 4)),
        ("select from trade where price ~between [price, price]", (9, 4)),
        ("select from quote where 1.1 ~between [ask, bid]", (1, 6)),
        (
            "select sym, ask_size, total_ask_size=sum(ask_size) ~over sym from quote",
            (15, 3),
        ),
    ],
)
def test_sql(src, expect, prepare_engine):
    engine = prepare_engine
    res = eval_src(src, 0, engine, Context(dict()))
    assert res.j_type == JType.DATAFRAME
    assert res.data.shape == expect
