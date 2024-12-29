from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import polars as pl

from .ast import JObj
from .constant import PL_DTYPE_TO_J_TYPE
from .exceptions import JasmineEvalException
from .j_fn import JFn


class JType(Enum):
    NULL = 0
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
        JObj
        | date
        | int
        | float
        | pl.Series
        | pl.DataFrame
        | JParted
        | pl.Expr
        | list
        | dict
        | JFn
    )
    j_type: JType

    def __init__(self, data: object, j_type=JType.NULL) -> None:
        self.data = data
        if isinstance(data, JObj):
            self.j_type = JType(data.j_type)
            match self.j_type:
                case JType.DATETIME | JType.TIMESTAMP:
                    self.data = data
                case JType.LIST:
                    py_list = data.as_py()
                    for i, j_obj in enumerate(py_list):
                        py_list[i] = J(j_obj, j_obj.j_type)
                    self.data = py_list
                case JType.DICT:
                    py_dict = data.as_py()
                    for k, v in py_dict.items():
                        py_dict[k] = J(v, v.j_type)
                    self.data = py_dict
                case _:
                    self.data = data.as_py()
        elif isinstance(data, bool):
            self.j_type = JType.BOOLEAN
        elif isinstance(data, int):
            if j_type == JType.NULL:
                self.j_type = JType.INT
            else:
                self.j_type = j_type
        elif isinstance(data, float):
            self.j_type = JType.FLOAT
        elif isinstance(data, str):
            if j_type == JType.NULL:
                self.j_type = JType.STRING
            else:
                self.j_type = j_type
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
        elif isinstance(data, dict):
            self.j_type = JType.DICT
        else:
            self.j_type = j_type

    def __str__(self) -> str:
        match JType(self.j_type):
            case JType.NULL:
                return "0n"
            case (
                JType.BOOLEAN
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
            case JType.LIST:
                output = "[\n"
                for j in self.data:
                    output += f"  {j.short_format()},\n"
                return output + "]"
            case JType.DICT:
                key_length = max(map(lambda x: len(x), self.data.keys())) + 1
                output = "{\n"
                for k, v in self.data.items():
                    output += (
                        f"  '{k}'{" " * (key_length-len(k))}: {v.short_format()},\n"
                    )
                output += "}"
                return output
            case JType.FN:
                return str(self.data)
            case _:
                return repr(self)

    def short_format(self) -> str:
        match JType(self.j_type):
            case JType.DICT:
                return f"{{{", ".join(self.data.keys())}}}"
            case JType.LIST:
                return f"[{", ".join(map(lambda x: x.short_format(), self.data))}]"
            case JType.SERIES:
                return f"[{PL_DTYPE_TO_J_TYPE.get(self.data.dtype, 'unknown')} series]"
            case JType.DATAFRAME:
                return f"df{self.data.shape}[{', '.join(self.data.columns)}]"
            case _:
                return str(self)

    def __repr__(self) -> str:
        return "<%s - %s>" % (self.j_type.name, self.data)

    def int(self) -> int:
        match self.j_type:
            case JType.BOOLEAN:
                return 1 if self.data else 0
            case JType.INT:
                return self.data
            case JType.DATE | JType.TIME | JType.DURATION:
                return self.data
            case JType.DATETIME | JType.TIMESTAMP:
                return self.data.as_py()
            case JType.FLOAT:
                return int(self.data)
            case JType.STRING:
                return int(self.data)
            case _:
                raise JasmineEvalException(
                    "expect 'INT', but got %s" % self.j_type.name
                )

    def float(self) -> float:
        if (
            self.j_type == JType.FLOAT
            or self.j_type == JType.INT
            or self.j_type == JType.BOOLEAN
        ):
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
                case JType.SERIES | JType.DATAFRAME:
                    return self.data.equals(
                        value.data,
                    )
                case _:
                    return self.data == value.data
        else:
            return False

    def __len__(self):
        match JType(self.j_type):
            case (
                JType.PARTED
                | JType.ERR
                | JType.FN
                | JType.MISSING
                | JType.RETURN
                | JType.PARTED
                | JType.EXPR
            ):
                return 0
            case JType.SERIES | JType.DATAFRAME | JType.LIST | JType.DICT:
                return len(self.data)
            case _:
                return 1

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
            case JType.NULL:
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
            case JType.NULL | JType.INT | JType.DATE | JType.FLOAT | JType.SERIES:
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

    def is_numeric_scalar(self) -> bool:
        return self.j_type in {JType.INT, JType.FLOAT, JType.BOOLEAN}

    def is_temporal_scalar(self) -> bool:
        if self.j_type.value >= 3 and self.j_type.value <= 7:
            return True
        else:
            return False

    def to_datetime(self) -> datetime:
        if self.j_type == JType.DATETIME or self.j_type == JType.TIMESTAMP:
            time_zone = self.data.tz()
            second = (
                self.data.as_py() / 1000
                if self.j_type == JType.DATETIME
                else self.data.as_py() / 1000_000_000
            )
            return datetime.fromtimestamp(second, tz=ZoneInfo(time_zone))
        else:
            raise JasmineEvalException(
                "expect 'DATETIME|TIMESTAMP', but got %s" % self.j_type.name
            )

    def to_list(self) -> list:
        if self.j_type == JType.SERIES:
            if isinstance(self.data.dtype, pl.Datetime):
                time_zone = self.data.dtype.time_zone
                if time_zone is None:
                    time_zone = "UTC"
                time_unit = self.data.dtype.time_unit
                if time_unit == "us":
                    raise JasmineEvalException("not support 'us' unit datetime")
                datetimes = []
                for n in list(self.data.cast(pl.Int64)):
                    datetimes.append(J(JObj(n, time_zone, time_unit)))
                return datetimes
            elif self.data.dtype == pl.Duration:
                durations = []
                for n in list(self.data.cast(pl.Int64)):
                    durations.append(J(n, JType.DURATION))
                return durations
            elif self.data.dtype == pl.Time:
                times = []
                for n in list(self.data.cast(pl.Int64)):
                    times.append(J(n, JType.TIME))
                return times
            else:
                j_list = []
                for n in list(self.data):
                    j_list.append(J(n))
                return j_list
        elif self.j_type == JType.DICT:
            return list(self.data.values())
        elif self.j_type == JType.LIST:
            return self.data
        else:
            return [self]

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

    def neg(self):
        if self.j_type == JType.NULL:
            return self
        elif self.j_type == JType.INT or self.j_type == JType.FLOAT:
            return J(-self.data, self.j_type)
        elif self.j_type == JType.DURATION:
            return J(-self.data, self.j_type)
        elif self.j_type == JType.SERIES:
            return J(-self.data, self.j_type)
        elif self.j_type == JType.LIST:
            return J([item.neg() for item in self.data], self.j_type)
        elif self.j_type == JType.DICT:
            return J({k: v.neg() for k, v in self.data.items()}, self.j_type)
        else:
            raise JasmineEvalException(
                "'neg' not supported for type %s" % self.j_type.name
            )

    def is_truthy(self) -> bool:
        match self.j_type:
            case JType.BOOLEAN:
                return self.data
            case JType.INT:
                return self.data != 0
            case JType.FLOAT:
                return self.data != 0.0
            case JType.STRING:
                return len(self.data) > 0
            case JType.LIST:
                return len(self.data) > 0
            case JType.DICT:
                return len(self.data) > 0
            case JType.SERIES | JType.DATAFRAME:
                return not self.data.is_empty()
            case _:
                return False


def date_to_num(dt: date) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day
