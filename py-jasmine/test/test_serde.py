from datetime import date

import polars as pl
import pytest

from jasminum import serde
from jasminum.j import J, JType
from jasminum.j_fn import JFn


@pytest.mark.parametrize(
    "j,msg_bytes",
    [
        (J(None), bytes(8)),
        (J(True), b"\x01\x00\x00\x00\x01\x00\x00\x00"),
        (J(1), b"\x02\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"),
        (J(date(2024, 12, 23)), bytes([3, 0, 0, 0, 112, 78, 0, 0])),
        (
            J(67859100000000, JType.TIME),
            bytes([4, 0, 0, 0, 0, 0, 0, 0, 0, 223, 140, 173, 183, 61, 0, 0]),
        ),
        (
            J.from_millis(788268494365, "Asia/Tokyo"),
            b"\x05\x00\x00\x00\x12\x00\x00\x00\x1d\xaev\x88\xb7\x00\x00\x00Asia/Tokyo\x00\x00\x00\x00\x00\x00",
        ),
        (
            J.from_nanos(788268474218211394, "Asia/Tokyo"),
            b"\x06\x00\x00\x00\x12\x00\x00\x00Bhj9\x00~\xf0\nAsia/Tokyo\x00\x00\x00\x00\x00\x00",
        ),
        (
            J(3141592653, JType.DURATION),
            b"\x07\x00\x00\x00\x00\x00\x00\x00M\xe6@\xbb\x00\x00\x00\x00",
        ),
        (J(3.14), b"\x08\x00\x00\x00\x00\x00\x00\x00\x1f\x85\xebQ\xb8\x1e\t@"),
        (J("Frieren"), b"\x09\x00\x00\x00\x07\x00\x00\x00Frieren\x00"),
        (J("Fern", JType.CAT), b"\n\x00\x00\x00\x04\x00\x00\x00Fern\x00\x00\x00\x00"),
        (
            J(pl.Series(None)),
            (
                b"\x0b\x00\x00\x00\x8b\x01\x00\x00ARROW1\x00\x00\xff\xff\xff\xffp\x00\x00\x00\x04\x00\x00"
                + b"\x00\xf2\xff\xff\xff\x14\x00\x00\x00\x04\x00\x01\x00\x00\x00\n"
                + b"\x00\x0b\x00\x08\x00\n\x00\x04\x00\xf8\xff\xff\xff\x0c\x00\x00"
                + b"\x00\x08\x00\x08\x00\x00\x00\x04\x00\x01\x00\x00\x00\x04\x00\x00"
                + b"\x00\xec\xff\xff\xff,\x00\x00\x00 \x00\x00\x00\x18\x00\x00\x00\x01\x01\x00"
                + b"\x00\x10\x00\x12\x00\x04\x00\x10\x00\x11\x00\x08\x00\x00\x00\x0c"
                + b"\x00\x00\x00\x00\x00\xfc\xff\xff\xff\x04\x00\x04\x00\x00\x00\x00"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xffX\x00\x00"
                + b"\x00\x08\x00\x00\x00\x00\x00\x00\x00\xf2\xff\xff\xff\x14\x00\x00"
                + b"\x00\x04\x00\x03\x00\x00\x00\n\x00\x0b\x00\x08\x00\n\x00\x04\x00\xf2\xff\xff"
                + b"\xff\x1c\x00\x00\x00\x10\x00\x00\x00\x00\x00\n\x00\x0c\x00\x00"
                + b"\x00\x04\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                + b"\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x08\x00\x00"
                + b"\x00\x00\x00\x00\x00\xec\xff\xff\xff@\x00\x00\x008\x00\x00\x00\x14\x00\x00"
                + b"\x00\x04\x00\x00\x00\x0c\x00\x12\x00\x10\x00\x04\x00\x08\x00\x0c"
                + b"\x00\x01\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00`\x00\x00"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                + b"\x00\x00\x00\x00\x00\xf8\xff\xff\xff\x0c\x00\x00\x00\x08\x00\x08"
                + b"\x00\x00\x00\x04\x00\x01\x00\x00\x00\x04\x00\x00\x00\xec\xff\xff"
                + b"\xff,\x00\x00\x00 \x00\x00\x00\x18\x00\x00\x00\x01\x01\x00\x00\x10\x00\x12"
                + b"\x00\x04\x00\x10\x00\x11\x00\x08\x00\x00\x00\x0c\x00\x00\x00\x00"
                + b"\x00\xfc\xff\xff\xff\x04\x00\x04\x00\x00\x00\x00\x00\x00\x99\x00\x00\x00AR"
                + b"ROW1\x00\x00\x00\x00\x00"
            ),
        ),
        (
            J(pl.DataFrame([None])),
            (
                b"\x0f\x00\x00\x00\xa3\x01\x00\x00ARROW1\x00\x00\xff\xff\xff\xffx\x00\x00\x00\x04\x00\x00"
                + b"\x00\xf2\xff\xff\xff\x14\x00\x00\x00\x04\x00\x01\x00\x00\x00\n"
                + b"\x00\x0b\x00\x08\x00\n\x00\x04\x00\xf8\xff\xff\xff\x0c\x00\x00"
                + b"\x00\x08\x00\x08\x00\x00\x00\x04\x00\x01\x00\x00\x00\x04\x00\x00"
                + b"\x00\xec\xff\xff\xff,\x00\x00\x00 \x00\x00\x00\x18\x00\x00\x00\x01\x01\x00"
                + b"\x00\x10\x00\x12\x00\x04\x00\x10\x00\x11\x00\x08\x00\x00\x00\x0c"
                + b"\x00\x00\x00\x00\x00\xfc\xff\xff\xff\x04\x00\x04\x00\x08\x00\x00\x00column_"
                + b"0\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff`\x00\x00\x00\x04\x00\x00"
                + b"\x00\xf2\xff\xff\xff\x14\x00\x00\x00\x04\x00\x03\x00\x00\x00\n"
                + b"\x00\x0b\x00\x08\x00\n\x00\x04\x00\xe6\xff\xff\xff\x01\x00\x00"
                + b"\x00\x00\x00\x00\x00 \x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\n"
                + b"\x00\x14\x00\x04\x00\x0c\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00"
                + b"\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00"
                + b"\x00\x08\x00\x00\x00\x00\x00\x00\x00\xec\xff\xff\xff@\x00\x00\x008\x00\x00"
                + b"\x00\x14\x00\x00\x00\x04\x00\x00\x00\x0c\x00\x12\x00\x10\x00\x04"
                + b"\x00\x08\x00\x0c\x00\x01\x00\x00\x00\x88\x00\x00\x00\x00\x00\x00"
                + b"\x00h\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\xff\xff\xff\x0c\x00\x00"
                + b"\x00\x08\x00\x08\x00\x00\x00\x04\x00\x01\x00\x00\x00\x04\x00\x00"
                + b"\x00\xec\xff\xff\xff,\x00\x00\x00 \x00\x00\x00\x18\x00\x00\x00\x01\x01\x00"
                + b"\x00\x10\x00\x12\x00\x04\x00\x10\x00\x11\x00\x08\x00\x00\x00\x0c"
                + b"\x00\x00\x00\x00\x00\xfc\xff\xff\xff\x04\x00\x04\x00\x08\x00\x00\x00column_"
                + b"0\x00\xa1\x00\x00\x00ARROW1\x00\x00\x00\x00\x00"
            ),
        ),
        (
            J([J(1), J("hello"), J(None)]),
            b"\r\x00\x00\x00"
            + b"0\x00\x00\x00"
            + b"\x03\x00\x00\x00\x00\x00\x00\x00"
            + b"\x02\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"
            + b"\x09\x00\x00\x00\x05\x00\x00\x00hello\x00\x00\x00"
            + b"\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
        (
            J({"a": J(1), "b": J("hello"), "c": J(None)}),
            b"\x0e\x00\x00\x00"
            + b"H\x00\x00\x00"
            + b"\x03\x00\x00\x00"
            + b"\x0f\x00\x00\x00"
            + b"\x01\x00\x00\x00"
            + b"\x02\x00\x00\x00"
            + b"\x03\x00\x00\x00"
            + b"abc\x00"
            + b"(\x00\x00\x00\x00\x00\x00\x00"
            + b"\x02\x00\x00\x00\x00\x00\x00\x00"
            + b"\x01\x00\x00\x00\x00\x00\x00\x00"
            + b"\x09\x00\x00\x00"
            + b"\x05\x00\x00\x00"
            + b"hello\x00\x00\x00"
            + b"\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
    ],
)
def test_serde(j, msg_bytes):
    assert serde.serialize(j, False) == msg_bytes
    assert serde.deserialize(msg_bytes) == j


def test_serde_fn():
    fn = J(JFn("sum", dict(), [], 0, "sum"))
    assert (
        serde.serialize(fn, False)
        == b"\x11\x00\x00\x00\x03\x00\x00\x00" + b"sum\x00\x00\x00\x00\x00"
    )
    assert (
        serde.deserialize(
            b"\x11\x00\x00\x00\x03\x00\x00\x00" + b"sum\x00\x00\x00\x00\x00"
        ).data.fn
        == "sum"
    )
