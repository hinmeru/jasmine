import polars as pl

from .exceptions import JasmineEvalException
from .j import J, JType


def abs(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().abs())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("abs", arg.j_type.name)
        )


def all(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().all())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("all", arg.j_type.name)
        )


def any(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().any())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("any", arg.j_type.name)
        )


def arccos(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().arccos())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("acos", arg.j_type.name)
        )


def arccosh(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().arccosh())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("acosh", arg.j_type.name)
        )


def arcsin(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().arcsin())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("asin", arg.j_type.name)
        )


def arcsinh(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().arcsinh())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("asinh", arg.j_type.name)
        )


def arctan(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().arctan())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("atan", arg.j_type.name)
        )


def arctanh(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().arctanh())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("atanh", arg.j_type.name)
        )


def cbrt(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cbrt())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cbrt", arg.j_type.name)
        )


def ceil(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().ceil())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("ceil", arg.j_type.name)
        )


def cos(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cos())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cos", arg.j_type.name)
        )


def cosh(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cosh())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cosh", arg.j_type.name)
        )


def cot(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cot())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cot", arg.j_type.name)
        )


def cmax(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cum_max())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cmax", arg.j_type.name)
        )


def cmin(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cum_min())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cmin", arg.j_type.name)
        )


def cprod(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cum_prod())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("cprod", arg.j_type.name)
        )


def csum(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().cum_sum())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("csum", arg.j_type.name)
        )


def diff(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().diff())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("diff", arg.j_type.name)
        )


def exp(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().exp())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("exp", arg.j_type.name)
        )


def floor(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().floor())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("floor", arg.j_type.name)
        )


def interp(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().interpolate())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "interp", arg.j_type.name
            )
        )


def kurtosis(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().kurtosis())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "kurtosis", arg.j_type.name
            )
        )


def ln(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().log())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("ln", arg.j_type.name)
        )


def log10(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().log10())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("log10", arg.j_type.name)
        )


def log1p(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().log1p())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("log1p", arg.j_type.name)
        )


def max(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().max())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("max", arg.j_type.name)
        )


def mean(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().mean())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("mean", arg.j_type.name)
        )


def median(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().median())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format(
                "median", arg.j_type.name
            )
        )


def min(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().min())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("min", arg.j_type.name)
        )


def mode(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().mode())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("mode", arg.j_type.name)
        )


def neg(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().neg())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("neg", arg.j_type.name)
        )


def not_(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().not_())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("not", arg.j_type.name)
        )


def pc(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().pct_change())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("pc", arg.j_type.name)
        )


def prod(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().product())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("prod", arg.j_type.name)
        )


def sign(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sign())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("sign", arg.j_type.name)
        )


def sin(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sin())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("sin", arg.j_type.name)
        )


def sinh(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sinh())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("sinh", arg.j_type.name)
        )


def skew(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().skew())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("skew", arg.j_type.name)
        )


def sqrt(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sqrt())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("sqrt", arg.j_type.name)
        )


def std0(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().std(0))
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("std0", arg.j_type.name)
        )


def std1(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().std())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("std1", arg.j_type.name)
        )


def sum(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().sum())
    elif arg.is_numeric_scalar():
        return arg
    elif arg.j_type == JType.SERIES:
        return J(arg.data.sum())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("sum", arg.j_type.name)
        )


def tan(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().tan())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("tan", arg.j_type.name)
        )


def tanh(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().tanh())
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("tanh", arg.j_type.name)
        )


def var0(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().var(0))
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("var0", arg.j_type.name)
        )


def var1(arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().var(1))
    else:
        raise JasmineEvalException(
            "unsupported operand type for '{0}': '{1}'".format("var1", arg.j_type.name)
        )


def floor_div(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(arg1.to_expr().floordiv(arg2.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "div", arg1.j_type.name, arg2.j_type.name
            )
        )


def corr0(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(pl.corr(arg1.to_expr(), arg2.to_expr(), ddof=0))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "corr0", arg1.j_type.name, arg2.j_type.name
            )
        )


def corr1(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(pl.corr(arg1.to_expr(), arg2.to_expr(), ddof=1))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "corr1", arg1.j_type.name, arg2.j_type.name
            )
        )


def cov0(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(pl.cov(arg1.to_expr(), arg2.to_expr(), ddof=0))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "cov0", arg1.j_type.name, arg2.j_type.name
            )
        )


def cov1(arg1: J, arg2: J) -> J:
    if arg1.j_type == JType.EXPR or arg2.j_type == JType.EXPR:
        return J(pl.cov(arg1.to_expr(), arg2.to_expr(), ddof=1))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "cov1", arg1.j_type.name, arg2.j_type.name
            )
        )


def emean(alpha: J, arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().ewm_mean(alpha.float()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "emean", alpha.j_type.name, arg.j_type.name
            )
        )


def estd(alpha: J, arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().ewm_std(alpha.float()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "estd", alpha.j_type.name, arg.j_type.name
            )
        )


def evar(alpha: J, arg: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(arg.to_expr().ewm_var(alpha.float()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "evar", alpha.j_type.name, arg.j_type.name
            )
        )


def log(arg: J, base: J) -> J:
    if arg.j_type == JType.EXPR or base.j_type == JType.EXPR:
        return J(arg.to_expr().log(base.float()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "log", arg.j_type.name, base.j_type.name
            )
        )


def rmax(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_max(windows_size.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rmax", windows_size.j_type.name, arg.j_type.name
            )
        )


def rmean(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_mean(windows_size.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rmean", windows_size.j_type.name, arg.j_type.name
            )
        )


def rmedian(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_median(windows_size.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rmedian", windows_size.j_type.name, arg.j_type.name
            )
        )


def rmin(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_min(windows_size.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rmin", windows_size.j_type.name, arg.j_type.name
            )
        )


def rskew(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_skew(windows_size.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rskew", windows_size.j_type.name, arg.j_type.name
            )
        )


def rstd0(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_std(windows_size.int(), ddof=0))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rstd0", windows_size.j_type.name, arg.j_type.name
            )
        )


def rstd1(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_std(windows_size.int(), ddof=1))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rstd1", windows_size.j_type.name, arg.j_type.name
            )
        )


def rsum(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_sum(windows_size.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rsum", windows_size.j_type.name, arg.j_type.name
            )
        )


def rvar0(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_var(windows_size.int(), ddof=0))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rvar0", windows_size.j_type.name, arg.j_type.name
            )
        )


def rvar1(windows_size: J, arg: J) -> J:
    if windows_size.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().rolling_var(windows_size.int(), ddof=1))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "rvar1", windows_size.j_type.name, arg.j_type.name
            )
        )


def quantile(arg: J, quantile: J) -> J:
    if arg.j_type == JType.EXPR or quantile.j_type == JType.EXPR:
        return J(arg.to_expr().quantile(quantile.float()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "quantile", arg.j_type.name, quantile.j_type.name
            )
        )


def round(arg: J, decimals: J) -> J:
    if arg.j_type == JType.EXPR or decimals.j_type == JType.EXPR:
        return J(arg.to_expr().round(decimals.int()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "round", arg.j_type.name, decimals.j_type.name
            )
        )


def wmean(weights: J, arg: J) -> J:
    if weights.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().dot(weights.to_expr()) / weights.to_expr().sum())
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "wmean", weights.j_type.name, arg.j_type.name
            )
        )


def wsum(weights: J, arg: J) -> J:
    if weights.j_type == JType.EXPR or arg.j_type == JType.EXPR:
        return J(arg.to_expr().dot(weights.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}' and '{2}'".format(
                "wsum", weights.j_type.name, arg.j_type.name
            )
        )


def clip(arg: J, lower: J, upper: J) -> J:
    if (
        arg.j_type == JType.EXPR
        or lower.j_type == JType.EXPR
        or upper.j_type == JType.EXPR
    ):
        return J(arg.to_expr().clip(lower.to_expr(), upper.to_expr()))
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}', '{2}' and '{3}'".format(
                "wsum", arg.j_type.name, lower.j_type.name, upper.j_type.name
            )
        )


def rquantile(windows_size: J, arg: J, quantile: J) -> J:
    if arg.j_type == JType.EXPR:
        return J(
            arg.to_expr().rolling_quantile(
                quantile.float(), window_size=windows_size.int()
            )
        )
    else:
        raise JasmineEvalException(
            "unsupported operand type(s) for '{0}': '{1}', '{2}' and '{3}'".format(
                "rquantile", windows_size.j_type.name, arg.j_type.name, quantile.j_type
            )
        )
