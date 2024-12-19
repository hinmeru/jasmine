import zoneinfo
from pathlib import Path
from typing import Callable

import polars as pl

from . import cfg, df, expr, io, iterator, math, series, sql, string
from . import operator as op
from .ast import print_trace
from .exceptions import JasmineEvalException
from .j import J, JParted, JType
from .j_fn import JFn
from .temporal import tz


class Engine:
    globals: dict[str, any]
    handles: dict[int, any]
    sources: dict[int, (str, str)]
    builtins: dict[str, any]

    def __init__(self) -> None:
        self.globals = dict()
        self.handles = dict()
        self.sources = dict()
        self.builtins = dict()

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

        # iterator
        self.register_builtin("each", iterator.each)

        # config
        self.register_builtin("cfg_strlen", cfg.strlen)
        self.register_builtin("cfg_tbl", cfg.tbl)

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
            )
        )

    def get_trace(self, source_id: int, pos: int, msg: str) -> str:
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

    def complete(self, text, state):
        for cmd in self.builtins.keys():
            if cmd.startswith(text):
                if not state:
                    return cmd
                else:
                    state -= 1
