from copy import copy
from typing import Callable

import polars as pl

from .ast import (
    Ast,
    AstAssign,
    AstBinOp,
    AstCall,
    AstDataFrame,
    AstDict,
    AstFn,
    AstId,
    AstIf,
    AstIndexAssign,
    AstList,
    AstMatrix,
    AstOp,
    AstRaise,
    AstReturn,
    AstSeries,
    AstSkip,
    AstSql,
    AstSqlBracket,
    AstTry,
    AstType,
    AstUnaryOp,
    AstWhile,
    JObj,
    downcast_ast_node,
    parse_source_code,
)
from .context import Context
from .engine import Engine
from .exceptions import JasmineEvalException
from .j import J, JType
from .util import date_to_num


def import_path(path: str, engine: Engine):
    pass


def eval_src(source_code: str, source_id: int, engine: Engine, ctx: Context) -> J:
    nodes = parse_source_code(source_code, source_id)
    res = J(None, JType.NULL)
    for node in nodes:
        res = eval_node(node, engine, ctx, False)
        if res == JType.RETURN:
            return res.data
    return res


def eval_node(node, engine: Engine, ctx: Context, is_in_fn=False, is_in_sql=False) -> J:
    if isinstance(node, Ast):
        node = downcast_ast_node(node)

    if isinstance(node, JObj):
        return J(node, node.j_type)
    elif isinstance(node, AstAssign):
        res = eval_node(node.exp, engine, ctx, is_in_fn, is_in_sql)
        if is_in_fn and "." not in node.id:
            ctx.locals[node.id] = res
        else:
            engine.globals[node.id] = res
        return res
    elif isinstance(node, AstId):
        if node.name in engine.builtins:
            return engine.builtins[node.name]
        elif node.name in ctx.locals:
            return ctx.locals[node.name]
        elif node.name in engine.globals:
            return engine.globals[node.name]
        elif node.name == "i" and is_in_sql:
            return J(pl.int_range(pl.len(), dtype=pl.UInt32).alias("i"))
        elif "." not in node.name and is_in_sql:
            return J(pl.col(node.name))
        else:
            raise JasmineEvalException(
                engine.get_trace(
                    node.source_id, node.start, "'%s' is not defined" % node.name
                )
            )
    elif isinstance(node, AstSqlBracket):
        exprs = []
        for ast in node.exps:
            res = eval_node(downcast_ast_node(ast), engine, ctx, is_in_fn, is_in_sql)
            exprs.append(res)
        if len(exprs) == 1:
            return exprs[0]
        else:
            return J(exprs)
    elif isinstance(node, AstSeries):
        j = eval_node(node.exp, engine, ctx, is_in_fn, is_in_sql)
        if is_in_sql:
            return J(j.to_expr().alias(node.name))
        else:
            return J(j.to_series().alias(node.name))
    elif isinstance(node, AstUnaryOp):
        op = downcast_ast_node(node.op)
        op_fn = eval_node(op, engine, ctx, is_in_fn, is_in_sql)
        exp = eval_node(node.exp, engine, ctx, is_in_fn, is_in_sql)
        return eval_fn(op_fn, engine, ctx, op.source_id, op.start, exp)
    elif isinstance(node, AstBinOp):
        op = downcast_ast_node(node.op)
        op_fn = eval_node(op, engine, ctx, is_in_fn, is_in_sql)
        lhs = eval_node(node.lhs, engine, ctx, is_in_fn, is_in_sql)
        rhs = eval_node(node.rhs, engine, ctx, is_in_fn, is_in_sql)
        return eval_fn(
            op_fn,
            engine,
            ctx,
            op.source_id,
            op.start,
            lhs,
            rhs,
        )
    elif isinstance(node, AstCall):
        f = downcast_ast_node(node.f)
        fn = eval_node(f, engine, ctx, is_in_fn, is_in_sql)
        fn_args = []
        for arg in node.args:
            fn_args.append(eval_node(arg, engine, ctx, is_in_fn, is_in_sql))
        return eval_fn(fn, engine, ctx, node.source_id, node.start, *fn_args)
    elif isinstance(node, AstOp):
        if node.name in engine.builtins:
            return engine.builtins.get(node.name)
        elif node.name in engine.globals:
            return engine.globals.get(node.name)
        else:
            raise JasmineEvalException(
                engine.get_trace(
                    node.source_id, node.start, "'%s' is not defined" % node.name
                )
            )
    elif isinstance(node, AstFn):
        raise JasmineEvalException("not yet implemented")
    elif isinstance(node, AstDataFrame):
        df = []
        for series in node.exps:
            series = eval_node(series, engine, ctx, is_in_fn, is_in_sql).to_series()
            df.append(series)
        return J(pl.DataFrame(df))
    elif isinstance(node, AstSeries):
        j = eval_node(node.exp, engine, ctx, is_in_fn, is_in_sql)
        series = j.to_series()
        series = series.rename(node.name)
        return J(series)
    elif isinstance(node, AstSql):
        return eval_sql(node, engine, ctx, node.source_id, node.start, is_in_fn)
    elif isinstance(node, AstSkip):
        return J(None, JType.MISSING)
    elif isinstance(node, AstReturn):
        return J(eval_node(node, engine, ctx, is_in_fn, is_in_sql), JType.RETURN)
    elif isinstance(node, AstRaise):
        err = eval_node(node, engine, ctx, is_in_fn, is_in_sql)
        raise JasmineEvalException(
            engine.get_trace(node.source_id, node.start, err.to_str())
        )
    else:
        raise JasmineEvalException("not yet implemented - %s" % node)


LIST_AGG_FN = set(
    [
        "all",
        "any",
        "first",
        "last",
        "count",
        "max",
        "min",
        "mean",
        "median",
        "min",
        "uc",
        "std0",
        "std1",
        "sum",
        "var0",
        "var1",
    ]
)


def eval_fn(j_fn: J, engine: Engine, ctx: Context, source_id: int, start: int, *args):
    try:
        if j_fn.j_type == JType.DATAFRAME:
            df = j_fn.data
            if len(args) == 1:
                return J(df[args[0].to_str()])
            elif len(args) == 2:
                return J(df[args[0].to_str()][args[1].int()])
            else:
                raise JasmineEvalException(
                    engine.get_trace(
                        source_id,
                        start,
                        "only support up two args(column and index) for dataframe, got %s args"
                        % len(args),
                    )
                )
        elif j_fn.j_type == JType.SERIES:
            s = j_fn.data
            if len(args) == 1:
                return J(s[args[0].int()])
            else:
                raise JasmineEvalException(
                    engine.get_trace(
                        source_id,
                        start,
                        "only support up to one arg(index) for series, got %s args"
                        % len(args),
                    )
                )
        elif j_fn.j_type == JType.DICT:
            d = j_fn.data
            if len(args) == 1:
                return d[args[0].to_str()]
            else:
                raise JasmineEvalException(
                    engine.get_trace(
                        source_id,
                        start,
                        "only support up to one arg(key) for dict, got %s args"
                        % len(args),
                    )
                )
        elif j_fn.j_type == JType.FN:
            fn = j_fn.data
            if fn.arg_num < len(args):
                raise engine.get_trace(
                    source_id,
                    start,
                    "takes %s arguments but %s were given" % (fn.arg_num, len(args)),
                )

            fn_args = fn.args
            missing_arg_names = fn.arg_names.copy()
            missing_arg_num = 0
            for i, arg in enumerate(args):
                if arg.j_type == JType.MISSING:
                    missing_arg_num += 1
                else:
                    fn_args[fn.arg_names[i]] = arg
                    missing_arg_names.remove(fn.arg_names[i])

            if missing_arg_num == 0 and fn.arg_num == len(args):
                if isinstance(fn.fn, Callable):
                    if fn.fn.__name__ == "each":
                        arg1 = fn_args["arg1"]
                        arg2 = fn_args["arg2"]
                        # each
                        if arg1.j_type == JType.FN and arg2.j_type == JType.EXPR:
                            if (
                                arg1.j_type == JType.FN
                                and arg1.data.is_built_in()
                                # use list to check if to return agg result, all, any etc?
                                and arg1.data.fn.__name__ in LIST_AGG_FN
                            ):
                                match arg1.data.fn.__name__:
                                    case "all":
                                        return J(arg2.to_expr().list.all())
                                    case "any":
                                        return J(arg2.to_expr().list.any())
                                    case "first":
                                        return J(arg2.to_expr().list.first())
                                    case "last":
                                        return J(arg2.to_expr().list.last())
                                    case "count":
                                        return J(arg2.to_expr().list.len())
                                    case "max":
                                        return J(arg2.to_expr().list.max())
                                    case "min":
                                        return J(arg2.to_expr().list.min())
                                    case "mean":
                                        return J(arg2.to_expr().list.mean())
                                    case "median":
                                        return J(arg2.to_expr().list.median())
                                    case "min":
                                        return J(arg2.to_expr().list.min())
                                    case "uc":
                                        return J(arg2.to_expr().list.n_unique())
                                    case "std0":
                                        return J(arg2.to_expr().list.std(0))
                                    case "std1":
                                        return J(arg2.to_expr().list.std(1))
                                    case "sum":
                                        return J(arg2.to_expr().list.sum())
                                    case "var0":
                                        return J(arg2.to_expr().list.var(0))
                                    case "var1":
                                        return J(arg2.to_expr().list.var(1))
                            else:
                                j = eval_fn(
                                    arg1, engine, ctx, source_id, start, J(pl.element())
                                )
                                return J(arg2.to_expr().list.eval(j.to_expr()))
                        else:
                            raise JasmineEvalException(
                                "not yet implement 'each' for %s and %s" % (arg1, arg2)
                            )
                    else:
                        return fn.fn(**fn_args)
                else:
                    return eval_node(fn.fn, engine, Context(fn_args), True)
            else:
                new_fn = copy(fn)
                new_fn.arg_names = missing_arg_names
                new_fn.arg_num = len(missing_arg_names)
                new_fn.args = fn_args
                return J(new_fn)
        else:
            raise JasmineEvalException(
                engine.get_trace(
                    source_id, start, "not able to apply arg(s) to %s" % j_fn
                )
            )
    except Exception as e:
        raise JasmineEvalException(engine.get_trace(source_id, start, str(e)))


# op: String,
# from: Ast,
# filters: Vec<Ast>,
# groups: Vec<Ast>,
# ops: Vec<Ast>,
# sorts: Vec<Ast>,
# take: Ast,
def eval_sql(
    sql: AstSql,
    engine: Engine,
    ctx: Context,
    source_id: int,
    start: int,
    is_in_fn: bool,
):
    try:
        j = eval_node(sql.from_df, engine, ctx, is_in_fn)
        if j.j_type == JType.DATAFRAME:
            df = j.data.lazy()
            if len(sql.filters) > 0:
                for node in sql.filters:
                    df = df.filter(
                        eval_node(node, engine, ctx, is_in_fn, True).to_expr()
                    )
        elif j.j_type == JType.PARTED:
            missing_part_err = JasmineEvalException(
                "dataframe partitioned by %s requires its partitioned unit condition('==', 'in' or 'between') as its first filter clause"
                % j.j_type.name
            )
            # partitioned table
            if len(sql.filters) > 0:
                first_filter = downcast_ast_node(sql.filters[0])
                if isinstance(first_filter, AstBinOp):
                    op = downcast_ast_node(first_filter.op)
                    lhs = downcast_ast_node(first_filter.lhs)
                    rhs = eval_node(first_filter.rhs, engine, ctx, is_in_fn)
                    if not (isinstance(lhs, AstId) and lhs.name == j.data.get_unit()):
                        raise missing_part_err
                    if op.name == "==" and rhs.j_type == JType.DATE:
                        date_num = rhs.date_num()
                        partitions = j.data.get_partition_paths(date_num, date_num)
                        if len(partitions) == 0:
                            partitions = [j.data.get_latest_path()]
                            df = pl.scan_parquet(partitions, n_rows=0)
                        else:
                            df = pl.scan_parquet(partitions)
                    elif (
                        op.name == "between"
                        and rhs.j_type == JType.SERIES
                        and rhs.data.dtype == pl.Date
                        and rhs.data.count() == 2
                        and rhs.data.null_count() == 0
                    ):
                        start_date = rhs.data[0]
                        end_date = rhs.data[1]
                        partitions = j.data.get_partition_paths(
                            date_to_num(start_date), date_to_num(end_date)
                        )
                        if len(partitions) == 0:
                            partitions = [j.data.get_latest_path()]
                            df = pl.scan_parquet(partitions, n_rows=0)
                        else:
                            df = pl.scan_parquet(partitions)
                    elif op.name == "in" and rhs.j_type == JType.DATE:
                        date_num = date_to_num(rhs.data)
                        if date_num in j.data.partitions:
                            df = pl.scan_parquet(
                                j.data.get_partition_paths_by_date_nums([date_num])
                            )
                        else:
                            df = pl.scan_parquet([j.data.get_latest_path()], n_rows=0)
                    elif op.name == "in" and (
                        rhs.j_type == JType.SERIES and rhs.data.dtype == pl.Date
                    ):
                        date_nums = []
                        for date in rhs.data:
                            if date:
                                date_num = date_to_num(date)
                                if date_num in j.data.partitions:
                                    date_nums.append(date_num)
                        partitions = j.data.get_partition_paths_by_date_nums(date_nums)
                        if len(partitions) > 0:
                            df = pl.scan_parquet(partitions)
                        else:
                            df = pl.scan_parquet([j.data.get_latest_path()], n_rows=0)
                    else:
                        raise missing_part_err
                else:
                    raise missing_part_err
            else:
                raise missing_part_err

            if len(sql.filters) > 1:
                for node in sql.filters[1:]:
                    df = df.filter(
                        eval_node(node, engine, ctx, is_in_fn, True).to_expr()
                    )
        elif j.j_type == JType.STRING or j.j_type == JType.STRING:
            path = j.to_str()
            if path.endswith(".csv") or path.endswith(".gz"):
                df = pl.scan_csv(path)
            elif path.endswith(".json"):
                df = pl.scan_ndjson(path)
            elif path.endswith(".parquet"):
                df = pl.scan_parquet(path)
            else:
                raise JasmineEvalException(
                    "only support file ends with 'csv|gz|json|parquet', got %s" % path
                )
        else:
            raise JasmineEvalException(
                "'from' requires 'dataframe|partitioned dataframe|csv|parquet|ndjson', got %s"
                % j.j_type
            )

        groups = []
        if len(sql.groups) > 0:
            if sql.group_type == "by":
                for node in sql.groups:
                    groups.append(
                        eval_node(node, engine, ctx, is_in_fn, True).to_expr()
                    )
            elif len(sql.groups) >= 2:
                interval = eval_node(sql.groups[0], engine, ctx, is_in_fn, True)
                if interval.j_type == JType.INT:
                    interval = f"{interval.int()}i"
                elif interval.j_type == JType.DURATION:
                    interval = f"{interval.data}ns"
                else:
                    interval = interval.to_str()
                index_column = eval_node(
                    sql.groups[1], engine, ctx, is_in_fn, True
                ).to_expr()
                group_by = []
                for node in sql.groups[2:]:
                    group_by.append(
                        eval_node(node, engine, ctx, is_in_fn, True).to_expr()
                    )
                groups.append(None)
            else:
                raise JasmineEvalException("'%s' requires 'interval' and 'column name'")

        ops = []
        if len(sql.ops) > 0:
            for node in sql.ops:
                j = eval_node(node, engine, ctx, is_in_fn, True)
                ops.append(j.to_expr())

        if len(groups) > 0:
            if sql.op == "select":
                # group by
                if sql.group_type == "by":
                    df = df.group_by(groups, maintain_order=True)
                elif sql.group_type == "dyn":
                    df = df.group_by_dynamic(
                        index_column, every=interval, group_by=group_by
                    )
                else:
                    df = df.rolling(index_column, period=interval, group_by=group_by)
                # agg
                if len(ops) == 0:
                    df = df.agg(pl.col("*").last())
                else:
                    df = df.agg(ops)
            elif sql.op == "update":
                if sql.group_type == "dyn" or sql.group_type == "rolling":
                    raise JasmineEvalException(
                        "not support '%s' with 'update'" % sql.group_type
                    )
                over_ops = []
                for op in ops:
                    over_ops.append(op.over(groups))
                df.with_columns(over_ops)
            else:
                raise JasmineEvalException(
                    engine.get_trace(
                        source_id,
                        start,
                        "not support 'delete' with '%s'" % sql.group_type,
                    )
                )
        elif len(ops) > 0:
            if sql.op == "select":
                df = df.select(ops)
            elif sql.op == "update":
                df = df.with_columns(ops)
            else:
                df = df.drop(ops)

        sorts = []
        descendings = []
        if len(sql.sorts) > 0:
            for sort in sql.sorts:
                sort = downcast_ast_node(sort).name
                if sort.startswith("-"):
                    sorts.append(sort[1:])
                    descendings.append(True)
                else:
                    sorts.append(sort)
                    descendings.append(False)
            df = df.sort(sorts, descending=descendings)

        take = eval_node(sql.take, engine, ctx, is_in_fn, True)
        if take.j_type == JType.INT:
            n = take.int()
            if n > 0:
                df = df.head(n)
            else:
                df = df.tail(n)
        elif take.j_type == JType.NULL:
            pass
        else:
            raise JasmineEvalException(
                engine.get_trace(
                    source_id, start, "requires 'int' for 'take', got %s" % take
                )
            )
        return J(df.collect())
    except Exception as e:
        # raise e
        raise JasmineEvalException(engine.get_trace(source_id, start, str(e)))
