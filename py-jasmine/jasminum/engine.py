import asyncio
import zoneinfo
from datetime import datetime
from pathlib import Path
from typing import Callable

import polars as pl

from . import cfg, debug, df, expr, io, iterator, math, series, sql, string
from . import operator as op
from .ast import get_timezone, print_trace
from .exceptions import JasmineEvalException
from .j import J, JParted, JType
from .j_fn import JFn
from .j_handle import JHandle
from .j_task import JTask
from .temporal import tz


class Engine:
    globals: dict[str, J]
    handles: dict[int, JHandle]
    # source_id -> (source_code, filepath)
    sources: dict[int, (str, str)]
    source_paths: dict[str, int]
    builtins: dict[str, J]
    timer_task: asyncio.Task
    timer_tasks: dict[int, JTask]

    def __init__(self) -> None:
        self.globals = dict()
        self.handles = dict()
        self.sources = dict()
        self.builtins = dict()
        self.source_paths = dict()
        self.timer_tasks = dict()

        # operator
        self.register_builtin("!=", op.not_equal)
        self.register_builtin("<=", op.less_equal)
        self.register_builtin(">=", op.great_equal)
        self.register_builtin(">", op.great_than)
        self.register_builtin("<", op.less_than)
        self.register_builtin("==", op.equal)
        # self.register_builtin("!", op)
        self.register_builtin("@", op.get)
        # self.register_builtin("..", op)
        self.register_builtin("$", op.cast)
        self.register_builtin("?", op.rand)
        self.register_builtin("++", op.concat_list)
        self.register_builtin("+", op.add)
        self.register_builtin("-", op.sub)
        self.register_builtin("**", op.pow)
        self.register_builtin("*", op.mul)
        self.register_builtin("/", op.true_div)
        self.register_builtin("%", op.mod)
        self.register_builtin("|", op.bin_max)
        self.register_builtin("&", op.bin_min)
        self.register_builtin("#", op.take)
        self.register_builtin("^", op.xor)
        self.register_builtin("..", op.range)

        # system
        self.register_builtin("load", lambda x: self.load_partitioned_df(x))

        # expr
        self.register_builtin("col", expr.col)
        self.register_builtin("lit", expr.lit)
        self.register_builtin("selector", expr.selector)

        # math
        self.register_builtin("abs", math.abs)
        self.register_builtin("all", math.all)
        self.register_builtin("any", math.any)
        self.register_builtin("acos", math.arccos)
        self.register_builtin("acosh", math.arccosh)
        self.register_builtin("asin", math.arcsin)
        self.register_builtin("asinh", math.arcsinh)
        self.register_builtin("atan", math.arctan)
        self.register_builtin("atanh", math.arctanh)
        self.register_builtin("cbrt", math.cbrt)
        self.register_builtin("ceil", math.ceil)
        self.register_builtin("cos", math.cos)
        self.register_builtin("cosh", math.cosh)
        self.register_builtin("cot", math.cot)
        self.register_builtin("cmax", math.cmax)
        self.register_builtin("cmin", math.cmin)
        self.register_builtin("cprod", math.cprod)
        self.register_builtin("csum", math.csum)
        self.register_builtin("diff", math.diff)
        self.register_builtin("exp", math.exp)
        self.register_builtin("floor", math.floor)
        self.register_builtin("interp", math.interp)
        self.register_builtin("kurtosis", math.kurtosis)
        self.register_builtin("ln", math.ln)
        self.register_builtin("log10", math.log10)
        self.register_builtin("log1p", math.log1p)
        self.register_builtin("max", math.max)
        self.register_builtin("mean", math.mean)
        self.register_builtin("median", math.median)
        self.register_builtin("min", math.min)
        self.register_builtin("neg", math.neg)
        self.register_builtin("mode", math.mode)
        self.register_builtin("not", math.not_)
        self.register_builtin("pc", math.pc)
        self.register_builtin("prod", math.prod)
        self.register_builtin("sign", math.sign)
        self.register_builtin("sin", math.sin)
        self.register_builtin("sinh", math.sinh)
        self.register_builtin("skew", math.skew)
        self.register_builtin("sqrt", math.sqrt)
        self.register_builtin("std0", math.std0)
        self.register_builtin("std1", math.std1)
        self.register_builtin("sum", math.sum)
        self.register_builtin("tan", math.tan)
        self.register_builtin("tanh", math.tanh)
        self.register_builtin("var0", math.var0)
        self.register_builtin("var1", math.var1)

        # binary
        self.register_builtin("tz", tz)
        self.register_builtin("corr0", math.corr0)
        self.register_builtin("corr1", math.corr1)
        self.register_builtin("cov0", math.cov0)
        self.register_builtin("cov1", math.cov1)
        self.register_builtin("emean", math.emean)
        self.register_builtin("estd", math.estd)
        self.register_builtin("evar", math.evar)
        self.register_builtin("log", math.log)
        self.register_builtin("rmax", math.rmax)
        self.register_builtin("rmean", math.rmean)
        self.register_builtin("rmedian", math.rmedian)
        self.register_builtin("rmin", math.rmin)
        self.register_builtin("rskew", math.rskew)
        self.register_builtin("rstd0", math.rstd0)
        self.register_builtin("rstd1", math.rstd1)
        self.register_builtin("rsum", math.rsum)
        self.register_builtin("rvar0", math.rvar0)
        self.register_builtin("rvar1", math.rvar1)
        self.register_builtin("quantile", math.quantile)
        self.register_builtin("round", math.round)
        self.register_builtin("wmean", math.wmean)
        self.register_builtin("wsum", math.wsum)

        # string
        self.register_builtin("lowercase", string.lowercase)
        self.register_builtin("strips", string.strips)
        self.register_builtin("stripe", string.stripe)
        self.register_builtin("string", string.string)
        self.register_builtin("strip", string.strip)
        self.register_builtin("uppercase", string.uppercase)
        self.register_builtin("like", string.like)
        self.register_builtin("matches", string.matches)
        self.register_builtin("join", string.join)
        self.register_builtin("split", string.split)
        self.register_builtin("replace", string.replace)
        self.register_builtin("extract", string.extract)
        self.register_builtin("parse_date", string.parse_date)
        self.register_builtin("parse_datetime", string.parse_datetime)

        # series
        self.register_builtin("asc", series.asc)
        self.register_builtin("bfill", series.bfill)
        self.register_builtin("count", series.count)
        self.register_builtin("ccount", series.ccount)
        self.register_builtin("desc", series.desc)
        self.register_builtin("first", series.first)
        self.register_builtin("flatten", series.flatten)
        self.register_builtin("ffill", series.ffill)
        self.register_builtin("hash", series.hash)
        self.register_builtin("last", series.last)
        self.register_builtin("next", series.next)
        self.register_builtin("isnull", series.isnull)
        self.register_builtin("prev", series.prev)
        self.register_builtin("rank", series.rank)
        self.register_builtin("reverse", series.reverse)
        self.register_builtin("shuffle", series.shuffle)
        self.register_builtin("unique", series.unique)
        self.register_builtin("uc", series.uc)
        self.register_builtin("bottom", series.bottom)
        self.register_builtin("differ", series.differ)
        self.register_builtin("top", series.top)
        self.register_builtin("fill", series.fill)
        self.register_builtin("in", series.in_)
        self.register_builtin("intersect", series.intersect)
        self.register_builtin("shift", series.shift)
        self.register_builtin("ss", series.ss)
        self.register_builtin("ssr", series.ssr)
        self.register_builtin("union", series.union)

        # df
        self.register_builtin("aj", df.aj)
        self.register_builtin("cj", df.cj)
        self.register_builtin("fj", df.fj)
        self.register_builtin("lj", df.lj)
        self.register_builtin("oj", df.oj)
        self.register_builtin("semi", df.semi)
        self.register_builtin("anti", df.anti)
        self.register_builtin("extend", df.extend)
        self.register_builtin("vstack", df.vstack)
        self.register_builtin("hstack", df.hstack)
        self.register_builtin("schema", df.schema)

        # other
        self.register_builtin("clip", math.clip)
        self.register_builtin("rquantile", math.rquantile)

        # sql only
        self.register_builtin("between", sql.is_between)
        self.register_builtin("over", sql.over)

        # io
        self.register_builtin("wpart", io.wpart)
        self.register_builtin("rparquet", io.rparquet)
        self.register_builtin("wparquet", io.wparquet)
        self.register_builtin("rcsv", io.rcsv)
        self.register_builtin("wcsv", io.wcsv)
        self.register_builtin("ls", io.ls)
        self.register_builtin("rm", io.rm)
        self.register_builtin("hopen", io.hopen)
        self.register_builtin("hclose", io.hclose)
        self.register_builtin("hsync", io.hsync)
        self.register_builtin("hasync", io.hasync)
        # iterator
        self.register_builtin("each", iterator.each)

        # config
        self.register_builtin("cfg_strlen", cfg.strlen)
        self.register_builtin("cfg_tbl", cfg.tbl)

        # sys
        self.register_builtin("handle", handle)

        # timer
        self.register_builtin("task", task)
        self.register_builtin("schedule", schedule)
        self.register_builtin("pause", pause)
        self.register_builtin("unpause", unpause)
        self.register_builtin("trigger", trigger)

        # debug
        self.register_builtin("show", debug.show)
        self.register_builtin("assert_eq", debug.assert_eq)
        self.register_builtin("assert", debug.assert_true)

        # vars
        self.builtins["timezone"] = J(
            pl.Series("timezone", sorted(list(zoneinfo.available_timezones())))
        )

    def register_builtin(self, name: str, fn: Callable) -> None:
        arg_num = fn.__code__.co_argcount
        self.builtins[name] = J(
            JFn(
                fn,
                dict(),
                list(fn.__code__.co_varnames[:arg_num]),
                arg_num,
                name,
            )
        )

    def get_trace(self, source_id: int, pos: int, msg: str) -> str:
        if source_id == -1:
            return msg
        source, path = self.sources.get(source_id)
        return print_trace(source, path, pos, msg)

    # YYYYMMDD_00
    # YYYY_00
    def load_partitioned_df(self, path: J) -> J:
        if path.j_type != JType.CAT and path.j_type != JType.STRING:
            raise JasmineEvalException(
                "'load' requires cat|string, got %s" % path.j_type
            )
        p = Path(path.data).resolve()
        frames = []
        for df_path in p.iterdir():
            # skip name starts with digit
            if df_path.name[0].isdigit():
                continue
            else:
                if df_path.is_file():
                    self.globals[df_path.name] = J(JParted(df_path, 0, []))
                    frames.append(df_path.name)
                else:
                    partitions = []
                    unit = 0
                    for partition in df_path.iterdir():
                        if unit == 0:
                            if len(partition.name) <= 8:
                                unit = 4
                            else:
                                unit = 8
                        partitions.append(int(partition.name[:unit]))
                    if len(partitions) > 0:
                        self.globals[df_path.name] = J(
                            JParted(df_path, unit, sorted(partitions))
                        )
                        frames.append(df_path.name)
        return J(pl.Series("", frames))

    def set_var(self, name: str, value: J) -> None:
        self.globals[name] = value

    def get_var(self, name: str) -> J:
        return self.globals.get(name, J(None))

    def has_var(self, name: str) -> bool:
        return name in self.globals

    def set_handle(self, handle_id: int, handle: JHandle) -> None:
        self.handles[handle_id] = handle

    def get_handle(self, handle_id: int) -> JHandle:
        return self.handles.get(handle_id)

    def has_handle(self, handle_id: int) -> bool:
        return handle_id in self.handles

    def remove_handle(self, handle_id: int) -> None:
        self.handles.pop(handle_id)

    def get_source(self, source_id: int) -> tuple[str, str]:
        return self.sources.get(source_id, ("", ""))

    def set_source(self, source_id: int, source: tuple[str, str]) -> None:
        self.sources[source_id] = source
        filepath = source[0]
        self.source_paths[filepath] = source_id

    def has_source(self, source_id: int) -> bool:
        return source_id in self.sources

    def get_max_handle_id(self) -> int:
        if len(self.handles) == 0:
            return 3
        else:
            return max(self.handles.keys()) + 1

    def list_handles(self) -> pl.DataFrame:
        handle_ids = []
        conn_types = []
        local_hosts = []
        ports = []
        directions = []
        for k, v in self.handles.items():
            if k > 0:
                handle_ids.append(k)
            else:
                handle_ids.append(None)
            conn_types.append(v._type)
            local_hosts.append(v._host)
            ports.append(v._port)
            directions.append(v._direction)
        return pl.DataFrame(
            [
                pl.Series("handle_id", handle_ids, dtype=pl.Int64),
                pl.Series("conn_type", conn_types, dtype=pl.Utf8),
                pl.Series("host", local_hosts, dtype=pl.Utf8),
                pl.Series("port", ports, dtype=pl.Int64),
                pl.Series("direction", directions, dtype=pl.Utf8),
            ]
        )

    def complete(self, text, state):
        for cmd in self.builtins.keys():
            if cmd.startswith(text):
                if not state:
                    return cmd
                else:
                    state -= 1

    def set_timer_task(self, task: asyncio.Task) -> None:
        self.timer_task = task

    def list_timer_tasks(self) -> pl.DataFrame:
        ids = []
        functions = []
        args = []
        start_times = []
        end_times = []
        intervals = []
        last_times = []
        next_times = []
        is_actives = []
        descriptions = []
        upd_times = []
        for k, v in self.timer_tasks.items():
            ids.append(k)
            functions.append(
                repr(v.function.data)
                if v.function.j_type == JType.FN
                else str(v.function)
            )
            args.append(str(v.args))
            start_times.append(v.start_time)
            end_times.append(v.end_time)
            intervals.append(v.interval)
            last_times.append(v.last_run)
            next_times.append(v.next_run)
            is_actives.append(v.is_active)
            descriptions.append(v.description)
            upd_times.append(v.upd_time)
        timezone = get_timezone()
        return pl.DataFrame(
            [
                pl.Series("id", ids, dtype=pl.Int64),
                pl.Series("function", functions, dtype=pl.Utf8),
                pl.Series("args", args, dtype=pl.Utf8),
                pl.Series(
                    "start_time",
                    start_times,
                    dtype=pl.Datetime("ms", time_zone=timezone),
                ),
                pl.Series(
                    "end_time",
                    end_times,
                    dtype=pl.Datetime("ms", time_zone=timezone),
                ),
                pl.Series("interval", intervals, dtype=pl.Duration("ns")),
                pl.Series(
                    "last_run", last_times, dtype=pl.Datetime("ms", time_zone=timezone)
                ),
                pl.Series(
                    "next_run", next_times, dtype=pl.Datetime("ms", time_zone=timezone)
                ),
                pl.Series("is_active", is_actives, dtype=pl.Boolean),
                pl.Series("description", descriptions, dtype=pl.Utf8),
                pl.Series(
                    "upd_time", upd_times, dtype=pl.Datetime("ms", time_zone=timezone)
                ),
            ]
        )

    def schedule_job(
        self,
        function: J,
        args: J,
        start_time: datetime,
        end_time: datetime | None,
        interval: int,
        description: str,
    ) -> int:
        if len(self.timer_tasks) == 0:
            job_id = 0
        else:
            job_id = max(self.timer_tasks.keys()) + 1
        j_task = JTask(
            function, args.to_list(), start_time, end_time, interval, description
        )
        self.timer_tasks[job_id] = j_task
        return job_id

    def pause_task(self, task_id: int) -> None:
        self.timer_tasks[task_id].is_active = False

    def unpause_task(self, task_id: int) -> None:
        self.timer_tasks[task_id].is_active = True

    def trigger_task(self, task_id: int) -> None:
        self.timer_tasks[task_id].next_run = datetime.now().astimezone()


def handle() -> J:
    pass


def task() -> J:
    pass


def schedule(
    function: J, args: J, start_time: J, end_time: J, interval: J, description: J
) -> J:
    pass


def unpause(task_id: J) -> J:
    pass


def pause(task_id: J) -> J:
    pass


def trigger(task_id: J) -> J:
    pass
