import contextlib
from enum import Enum

with contextlib.suppress(ImportError):
    from jasminum.jasminum import (
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
        AstTry,
        AstUnaryOp,
        AstWhile,
        JObj,
        get_timezone,
        parse_source_code,
        print_trace,
    )


class AstType(Enum):
    J = 0
    Fn = 1
    UnaryOp = 2
    BinOp = 3
    Assign = 4
    IndexAssign = 5
    Op = 6
    Id = 7
    Call = 8
    If = 9
    While = 10
    Try = 11
    Return = 12
    Raise = 13
    Dataframe = 14
    Matrix = 15
    Dict = 16
    List = 17
    Series = 18
    Sql = 19
    Skip = 20


def downcast_ast_node(node: Ast):
    ast_type = AstType(node.get_ast_type())
    match ast_type:
        case AstType.J:
            return node.j()
        case AstType.Fn:
            return node.fn()
        case AstType.UnaryOp:
            return node.unary_op()
        case AstType.BinOp:
            return node.bin_op()
        case AstType.Assign:
            return node.assign()
        case AstType.IndexAssign:
            return node.index_assign()
        case AstType.Op:
            return node.op()
        case AstType.Id:
            return node.id()
        case AstType.Call:
            return node.call()
        case AstType.If:
            return node.if_exp()
        case AstType.While:
            return node.while_exp()
        case AstType.Try:
            return node.try_exp()
        case AstType.Return:
            return node.return_exp()
        case AstType.Raise:
            return node.raise_exp()
        case AstType.Dataframe:
            return node.dataframe()
        case AstType.Matrix:
            return node.matrix()
        case AstType.Dict:
            return node.dict()
        case AstType.List:
            return node.list()
        case AstType.Series:
            return node.series()
        case AstType.Sql:
            return node.sql()
        case AstType.Skip:
            return node.skip()


all = [
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
    AstTry,
    AstUnaryOp,
    AstWhile,
    JObj,
    parse_source_code,
    print_trace,
    get_timezone,
]
