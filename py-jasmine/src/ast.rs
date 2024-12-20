use jasmine::AstNode;
use pyo3::{pyclass, pymethods, PyResult};

use crate::{error::PyJasmineErr, j::JObj};

#[pyclass]
#[derive(Clone)]
pub struct Ast(AstNode);

impl Ast {
    pub fn new(ast: AstNode) -> Self {
        Self(ast)
    }
}

#[derive(Clone, PartialEq)]
pub enum AstType {
    J,
    Fn,
    UnaryOp,
    BinOp,
    Assign,
    IndexAssign,
    Op,
    Id,
    Call,
    If,
    While,
    Try,
    Return,
    Raise,
    Dataframe,
    Matrix,
    Dict,
    List,
    Series,
    Sql,
    SqlBracket,
    Skip,
}

#[pymethods]
impl Ast {
    pub fn get_ast_type(&self) -> u8 {
        let ast_type = match &self.0 {
            AstNode::J(_) => AstType::J,
            AstNode::Fn { .. } => AstType::Fn,
            AstNode::UnaryOp { .. } => AstType::UnaryOp,
            AstNode::BinOp { .. } => AstType::BinOp,
            AstNode::Assign { .. } => AstType::Assign,
            AstNode::IndexAssign { .. } => AstType::IndexAssign,
            AstNode::Op { .. } => AstType::Op,
            AstNode::Id { .. } => AstType::Id,
            AstNode::Call { .. } => AstType::Call,
            AstNode::If { .. } => AstType::If,
            AstNode::While { .. } => AstType::While,
            AstNode::Try { .. } => AstType::Try,
            AstNode::Return(_) => AstType::Return,
            AstNode::Raise { .. } => AstType::Raise,
            AstNode::Dataframe { .. } => AstType::Dataframe,
            AstNode::Matrix(_) => AstType::Matrix,
            AstNode::Dict { .. } => AstType::Dict,
            AstNode::List(..) => AstType::List,
            AstNode::Series { .. } => AstType::Series,
            AstNode::Sql { .. } => AstType::Sql,
            AstNode::SqlBracket(..) => AstType::SqlBracket,
            AstNode::Skip => AstType::Skip,
        };
        ast_type as u8
    }

    pub fn j(&self) -> PyResult<JObj> {
        if let AstNode::J(j) = &self.0 {
            Ok(JObj::new(j.clone()))
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "j obj",
                self.get_ast_type()
            )))
        }
    }

    pub fn id(&self) -> PyResult<AstId> {
        if let AstNode::Id {
            name,
            start,
            source_id,
        } = &self.0
        {
            Ok(AstId {
                name: name.to_string(),
                start: *start,
                source_id: *source_id,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast id",
                self.get_ast_type()
            )))
        }
    }

    pub fn r#fn(&self) -> PyResult<AstFn> {
        if let AstNode::Fn {
            stmts,
            arg_names,
            fn_body,
            start,
            source_id,
        } = &self.0
        {
            Ok(AstFn {
                stmts: stmts.into_iter().map(|n| Ast(n.clone())).collect(),
                arg_names: arg_names.to_vec(),
                fn_body: fn_body.to_string(),
                start: *start,
                source_id: *source_id,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast id",
                self.get_ast_type()
            )))
        }
    }

    pub fn unary_op(&self) -> PyResult<AstUnaryOp> {
        if let AstNode::UnaryOp { op, exp } = &self.0 {
            Ok(AstUnaryOp {
                op: Ast(*op.clone()),
                exp: Ast(*exp.clone()),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast unary op",
                self.get_ast_type()
            )))
        }
    }

    pub fn bin_op(&self) -> PyResult<AstBinOp> {
        if let AstNode::BinOp { op, lhs, rhs } = &self.0 {
            Ok(AstBinOp {
                op: Ast(*op.clone()),
                lhs: Ast(*lhs.clone()),
                rhs: Ast(*rhs.clone()),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast bin op",
                self.get_ast_type()
            )))
        }
    }

    pub fn assign(&self) -> PyResult<AstAssign> {
        if let AstNode::Assign { id, exp } = &self.0 {
            Ok(AstAssign {
                id: id.to_string(),
                exp: Ast(*exp.clone()),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast assign",
                self.get_ast_type()
            )))
        }
    }

    pub fn index_assign(&self) -> PyResult<AstIndexAssign> {
        if let AstNode::IndexAssign { id, indices, exp } = &self.0 {
            Ok(AstIndexAssign {
                id: id.to_string(),
                indices: indices.into_iter().map(|n| Ast(n.clone())).collect(),
                exp: Ast(*exp.clone()),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast index assign",
                self.get_ast_type()
            )))
        }
    }

    pub fn op(&self) -> PyResult<AstOp> {
        if let AstNode::Op {
            name,
            start,
            source_id,
        } = &self.0
        {
            Ok(AstOp {
                name: name.to_string(),
                start: *start,
                source_id: *source_id,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast op",
                self.get_ast_type()
            )))
        }
    }

    pub fn call(&self) -> PyResult<AstCall> {
        if let AstNode::Call {
            f,
            args,
            start,
            source_id,
        } = &self.0
        {
            Ok(AstCall {
                f: Ast(*f.clone()),
                args: args.into_iter().map(|n| Ast(n.clone())).collect(),
                start: *start,
                source_id: *source_id,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast call",
                self.get_ast_type()
            )))
        }
    }

    pub fn if_exp(&self) -> PyResult<AstIf> {
        if let AstNode::If { cond, stmts } = &self.0 {
            Ok(AstIf {
                cond: Ast(*cond.clone()),
                stmts: stmts.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast if",
                self.get_ast_type()
            )))
        }
    }

    pub fn while_exp(&self) -> PyResult<AstWhile> {
        if let AstNode::While { cond, stmts } = &self.0 {
            Ok(AstWhile {
                cond: Ast(*cond.clone()),
                stmts: stmts.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast while",
                self.get_ast_type()
            )))
        }
    }

    pub fn try_exp(&self) -> PyResult<AstTry> {
        if let AstNode::Try {
            tries,
            err,
            catches,
        } = &self.0
        {
            Ok(AstTry {
                tries: tries.into_iter().map(|n| Ast(n.clone())).collect(),
                err: err.to_string(),
                catches: catches.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast try",
                self.get_ast_type()
            )))
        }
    }

    pub fn return_exp(&self) -> PyResult<AstReturn> {
        if let AstNode::Return(node) = &self.0 {
            Ok(AstReturn {
                exp: Ast(*node.clone()),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast return",
                self.get_ast_type()
            )))
        }
    }

    pub fn raise_exp(&self) -> PyResult<AstRaise> {
        if let AstNode::Raise {
            exp,
            start,
            source_id,
        } = &self.0
        {
            Ok(AstRaise {
                exp: Ast(*exp.clone()),
                start: *start,
                source_id: *source_id,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast raise",
                self.get_ast_type()
            )))
        }
    }

    pub fn dataframe(&self) -> PyResult<AstDataFrame> {
        if let AstNode::Dataframe {
            exps,
            start,
            source_id,
        } = &self.0
        {
            Ok(AstDataFrame {
                exps: exps.into_iter().map(|n| Ast(n.clone())).collect(),
                start: *start,
                source_id: *source_id,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast dataframe",
                self.get_ast_type()
            )))
        }
    }

    pub fn matrix(&self) -> PyResult<AstMatrix> {
        if let AstNode::Matrix(nodes) = &self.0 {
            Ok(AstMatrix {
                exps: nodes.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast matrix",
                self.get_ast_type()
            )))
        }
    }

    pub fn dict(&self) -> PyResult<AstDict> {
        if let AstNode::Dict { keys, values } = &self.0 {
            Ok(AstDict {
                keys: keys.clone(),
                values: values.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast dict",
                self.get_ast_type()
            )))
        }
    }

    pub fn list(&self) -> PyResult<AstList> {
        if let AstNode::List(nodes) = &self.0 {
            Ok(AstList {
                exps: nodes.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast list",
                self.get_ast_type()
            )))
        }
    }

    pub fn series(&self) -> PyResult<AstSeries> {
        if let AstNode::Series { name, exp } = &self.0 {
            Ok(AstSeries {
                name: name.to_string(),
                exp: Ast(*exp.clone()),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast series",
                self.get_ast_type()
            )))
        }
    }

    pub fn sql(&self) -> PyResult<AstSql> {
        if let AstNode::Sql {
            op,
            from,
            filters,
            groups,
            group_type,
            ops,
            sorts,
            take,
            source_id,
            start,
        } = &self.0
        {
            Ok(AstSql {
                op: op.to_owned(),
                from_df: Ast(*from.clone()),
                filters: filters.into_iter().map(|n| Ast(n.clone())).collect(),
                groups: groups.into_iter().map(|n| Ast(n.clone())).collect(),
                group_type: group_type.to_owned(),
                ops: ops.into_iter().map(|n| Ast(n.clone())).collect(),
                sorts: sorts.into_iter().map(|n| Ast(n.clone())).collect(),
                take: Ast(*take.clone()),
                source_id: *source_id,
                start: *start,
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast sql",
                self.get_ast_type()
            )))
        }
    }

    pub fn sql_bracket(&self) -> PyResult<AstSqlBracket> {
        if let AstNode::SqlBracket(nodes) = &self.0 {
            Ok(AstSqlBracket {
                exps: nodes.into_iter().map(|n| Ast(n.clone())).collect(),
            })
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "ast sql bracket",
                self.get_ast_type()
            )))
        }
    }

    pub fn skip(&self) -> PyResult<AstSkip> {
        if let AstNode::Skip = &self.0 {
            Ok(AstSkip {})
        } else {
            Err(PyJasmineErr::new_err(format!(
                "failed to refer {0} from {1}",
                "skip",
                self.get_ast_type()
            )))
        }
    }
}

#[pyclass]
pub struct AstSkip();

#[pyclass(get_all)]
pub struct AstId {
    name: String,
    start: usize,
    source_id: usize,
}

#[pyclass(get_all)]
pub struct AstFn {
    stmts: Vec<Ast>,
    arg_names: Vec<String>,
    fn_body: String,
    start: usize,
    source_id: usize,
}

#[pyclass(get_all)]
pub struct AstUnaryOp {
    op: Ast,
    exp: Ast,
}

#[pyclass(get_all)]
pub struct AstBinOp {
    op: Ast,
    lhs: Ast,
    rhs: Ast,
}

#[pyclass(get_all)]
pub struct AstAssign {
    id: String,
    exp: Ast,
}

#[pyclass(get_all)]
pub struct AstIndexAssign {
    id: String,
    indices: Vec<Ast>,
    exp: Ast,
}

#[pyclass(get_all)]
#[derive(Clone)]
pub struct AstOp {
    name: String,
    start: usize,
    source_id: usize,
}

#[pyclass(get_all)]
pub struct AstCall {
    f: Ast,
    args: Vec<Ast>,
    start: usize,
    source_id: usize,
}

#[pyclass(get_all)]
pub struct AstIf {
    cond: Ast,
    stmts: Vec<Ast>,
}

#[pyclass(get_all)]

pub struct AstWhile {
    cond: Ast,
    stmts: Vec<Ast>,
}

#[pyclass(get_all)]
pub struct AstTry {
    tries: Vec<Ast>,
    err: String,
    catches: Vec<Ast>,
}

#[pyclass(get_all)]
pub struct AstReturn {
    exp: Ast,
}

#[pyclass(get_all)]
pub struct AstRaise {
    exp: Ast,
    start: usize,
    source_id: usize,
}

#[pyclass(get_all)]
pub struct AstDataFrame {
    exps: Vec<Ast>,
    start: usize,
    source_id: usize,
}

#[pyclass(get_all)]
pub struct AstMatrix {
    exps: Vec<Ast>,
}

#[pyclass(get_all)]
pub struct AstDict {
    keys: Vec<String>,
    values: Vec<Ast>,
}

#[pyclass(get_all)]
pub struct AstList {
    exps: Vec<Ast>,
}

#[pyclass(get_all)]
pub struct AstSeries {
    name: String,
    exp: Ast,
}

#[pyclass(get_all)]
pub struct AstSql {
    op: String,
    from_df: Ast,
    filters: Vec<Ast>,
    groups: Vec<Ast>,
    group_type: String,
    ops: Vec<Ast>,
    sorts: Vec<Ast>,
    take: Ast,
    source_id: usize,
    start: usize,
}

#[pyclass(get_all)]
pub struct AstSqlBracket {
    exps: Vec<Ast>,
}
