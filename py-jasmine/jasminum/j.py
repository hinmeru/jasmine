from datetime import date
from enum import Enum
from pathlib import Path
from typing import Literal

import polars as pl

from .ast import JObj
from .exceptions import JasmineEvalException
from .j_fn import JFn
from .util import date_to_num


class JType(Enum):
    NONE = 0
    BOOLEAN = 1
    INT = 2
    DATE = 3
    TIME = 4
    DATETIME = 5
    TIMESTAMP = 6
    DURATION = 7
    FLOAT = 8
    STRING = 9
    CAT = 10
    SERIES = 11
    MATRIX = 12
    LIST = 13
    DICT = 14
    DATAFRAME = 15
    ERR = 16
    FN = 17
    MISSING = 18
    RETURN = 19
    PARTED = 20
    EXPR = 21


class JParted:
    path: Path
    # single=0, date=4, year=8
    unit: int
    partitions: list[int]

    def __init__(
        self, path: Path, unit: Literal[0, 4, 8], partitions: list[int]
    ) -> None:
        self.path = path
        self.unit = unit
        self.partitions = partitions

    def get_unit(self) -> str:
        match self.unit:
            case 4:
                return "year"
            case 8:
                return "date"
            case _:
                return "single"

    def __str__(self) -> str:
        unit = self.get_unit()
        return f"partitioned by {unit} @ `{self.path}` - {self.partitions[-3:]}"

    def get_partition_paths(self, start: int, end: int) -> list[str]:
        paths = []
        for partition in filter(lambda x: x >= start and x <= end, self.partitions):
            paths.append(self.path.joinpath(str(partition) + "*"))
        return paths

    def get_latest_path(self) -> str:
        return self.path.joinpath(str(self.partitions[-1]) + "*")

    def get_partition_paths_by_date_nums(self, nums: list[int]) -> list[str]:
        paths = []
        for num in nums:
            if num in self.partitions:
                paths.append(self.path.joinpath(str(num) + "*"))
        return paths


class J:
    data: (
        JObj | date | int | float | pl.Series | pl.DataFrame | JParted | pl.Expr | list
    )
    j_type: JType

    def __init__(self, data, j_type=JType.NONE) -> None:
        self.data = data
        if isinstance(data, JObj):
            self.j_type = JType(data.j_type)
            match self.j_type:
                case JType.DATETIME | JType.TIMESTAMP:
                    self.data = data
                case _:
                    self.data = data.as_py()
        elif isinstance(data, pl.Series):
            self.j_type = JType.SERIES
        elif isinstance(data, pl.DataFrame):
            self.j_type = JType.DATAFRAME
        elif isinstance(data, JFn):
            self.j_type = JType.FN
        elif isinstance(data, date):
            self.j_type = JType.DATE
        elif isinstance(data, JParted):
            self.j_type = JType.PARTED
        elif isinstance(data, pl.Expr):
            self.j_type = JType.EXPR
        elif isinstance(data, list):
            self.j_type = JType.LIST
        else:
            self.j_type = j_type

    def __str__(self) -> str:
        match JType(self.j_type):
            case (
                JType.NONE
                | JType.BOOLEAN
                | JType.INT
                | JType.FLOAT
                | JType.SERIES
                | JType.DATAFRAME
            ):
                return f"{self.data}"
            case JType.STRING:
                return f'"{self.data}"'
            case JType.CAT:
                return f"'{self.data}'"
            case JType.DATE:
                return self.data.isoformat()
            case JType.TIME:
                sss = self.data % 1000000000
                ss = self.data // 1000000000
                HH = ss // 3600
                mm = ss % 3600 // 60
                ss = ss % 60
                return f"{HH:02d}:{mm:02d}:{ss:02d}:{sss:09d}"
            case JType.DATETIME:
                return self.data.format_temporal()
            case JType.TIMESTAMP:
                return self.data.format_temporal()
            case JType.DURATION:
                neg = "" if self.data >= 0 else "-"
                ns = abs(self.data)
                sss = ns % 1000000000
                ss = ns // 1000000000
                mm = ss // 60
                ss = ss % 60
                HH = mm // 60
                mm = mm % 60
                days = HH // 24
                HH = HH % 24
                return f"{neg}{days}D{HH:02d}:{mm:02d}:{ss:02d}:{sss:09d}"
            case JType.PARTED:
                return str(self.data)
            case _:
                return repr(self)

    def __repr__(self) -> str:
        return "<%s - %s>" % (self.j_type.name, self.data)

    def int(self) -> int:
        return int(self.data)

    def to_float(self) -> float:
        if self.j_type == JType.FLOAT or self.j_type == JType.INT:
            return float(self.data)
        else:
            raise JasmineEvalException("expect 'FLOAT', but got %s" % self.j_type.name)

    def days(self) -> int:
        if self.j_type == JType.DURATION:
            return self.data // 86_400_000_000_000
        else:
            raise JasmineEvalException(
                "requires 'duration' for 'days', got %s" % repr(self.j_type)
            )

    # -> YYYYMMDD number
    def date_num(self) -> int:
        if self.j_type == JType.DATE:
            return date_to_num(self.data)
        else:
            raise JasmineEvalException(
                "requires 'date' for 'date_num', got %s" % repr(self.j_type)
            )

    def days_from_epoch(self) -> int:
        if self.j_type == JType.DATE:
            return self.data.toordinal() - 719_163
        else:
            raise JasmineEvalException(
                "requires 'date' for 'days from epoch', got %s" % repr(self.j_type)
            )

    def nanos_from_epoch(self) -> int:
        if self.j_type == JType.DATE:
            return (self.data.toordinal() - 719_163) * 86_400_000_000_000
        if self.j_type == JType.TIMESTAMP:
            return self.data.as_py()
        else:
            raise JasmineEvalException(
                "requires 'date' or 'timestamp' for 'nanos from epoch', got %s"
                % repr(self.j_type)
            )

    def __eq__(self, value: object) -> bool:
        if isinstance(value, J):
            if self.j_type != value.j_type:
                return False
            match self.j_type:
                case JType.DATETIME | JType.TIMESTAMP:
                    return (
                        self.data.tz() == self.data.tz()
                        and self.data.as_py() == self.data.as_py()
                    )
                case _:
                    return self.data == value.data
        else:
            return False

    def with_timezone(self, timezone: str):
        return J(self.data.with_timezone(timezone), self.j_type)

    @classmethod
    def from_nanos(cls, ns: int, timezone: str):
        return J(JObj(ns, timezone, "ns"))

    @classmethod
    def from_millis(cls, ms: int, timezone: str):
        return J(JObj(ms, timezone, "ms"))

    def tz(self) -> str:
        return self.data.tz()

    def to_series(self) -> pl.Series:
        match self.j_type:
            case JType.NONE:
                return pl.Series("", [None], pl.Null)
            case JType.INT | JType.FLOAT | JType.DATE:
                return pl.Series("", [self.data])
            case JType.SERIES:
                return self.data
            case JType.TIME:
                return pl.Series("", [self.data], pl.Time)
            case JType.DATETIME | JType.TIMESTAMP:
                return self.data.as_series()
            case JType.DURATION:
                return pl.Series("", [self.data], pl.Duration("ns"))
            case JType.STRING:
                return pl.Series("", [self.data])
            case JType.CAT:
                return pl.Series("", [self.data], pl.Categorical)
            case _:
                # MATRIX | LIST | DICT | DATAFRAME | ERR | FN | MISSING | RETURN | PARTED
                raise JasmineEvalException(
                    "not supported to be used as a series: %s" % self.j_type.name
                )

    def to_expr(self) -> pl.Expr:
        match self.j_type:
            case JType.NONE | JType.INT | JType.DATE | JType.FLOAT | JType.SERIES:
                return pl.lit(self.data)
            case JType.TIME:
                return pl.lit(pl.Series("", [self.data], pl.Time))
            case JType.DATETIME | JType.TIMESTAMP:
                return pl.lit(self.data.as_series())
            case JType.DURATION:
                return pl.lit(pl.Series("", [self.data], pl.Duration("ns")))
            case JType.STRING | JType.CAT:
                return pl.lit(self.data)
            case JType.EXPR:
                return self.data
            case _:
                # MATRIX | LIST | DICT | DATAFRAME | ERR | FN | MISSING | RETURN | PARTED
                raise JasmineEvalException(
                    "not supported j type for sql fn: %s" % self.j_type.name
                )

    def to_exprs(self) -> list[pl.Expr]:
        if self.j_type == JType.LIST:
            return list(map(lambda x: x.to_expr(), self.data))
        else:
            return [self.to_expr()]

    def is_temporal_scalar(self) -> bool:
        if self.j_type.value >= 3 and self.j_type.value <= 7:
            return True
        else:
            return False

    def to_str(self) -> str:
        if self.j_type == JType.STRING or self.j_type == JType.CAT:
            return self.data
        else:
            raise JasmineEvalException(
                "expect 'STRING|CAT', but got %s" % self.j_type.name
            )

    def to_strs(self) -> list[str]:
        if self.j_type == JType.STRING or self.j_type == JType.CAT:
            return [self.data]
        elif self.j_type == JType.SERIES and self.data.is_empty():
            return []
        elif self.j_type == JType.SERIES and (
            self.data.dtype == pl.String or self.data.dtype == pl.Categorical
        ):
            return self.data.to_list()
        else:
            raise JasmineEvalException(
                "expect 'STRING|CAT|STRINGS|CATS', but got %s" % self.j_type.name
            )

    def to_bool(self) -> bool:
        if self.j_type == JType.BOOLEAN:
            return self.data
        else:
            raise JasmineEvalException(
                "expect 'BOOLEAN', but got %s" % self.j_type.name
            )

    def to_df(self) -> pl.DataFrame:
        if self.j_type == JType.DATAFRAME:
            return self.data
        else:
            raise JasmineEvalException(
                "expect 'DATAFRAME', but got %s" % self.j_type.name
            )

    def assert_types(self, types: list[JType]):
        if self.j_type not in types:
            raise JasmineEvalException(
                "expect '%s', but got %s"
                % ("|".join(map(lambda x: x.name, types)), self.j_type)
            )

    def assert_type(self, type: JType):
        if self.j_type == type:
            raise JasmineEvalException(
                "expect '%s', but got %s" % (type.name, self.j_type)
            )
