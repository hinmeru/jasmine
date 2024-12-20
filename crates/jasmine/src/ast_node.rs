use crate::errors::{JError, JResult};
use crate::j::J;

#[derive(PartialEq, Debug, Clone)]
pub enum AstNode {
    J(J),
    Fn {
        stmts: Vec<AstNode>,
        arg_names: Vec<String>,
        fn_body: String,
        start: usize,
        source_id: usize,
    },
    UnaryOp {
        op: Box<AstNode>,
        exp: Box<AstNode>,
    },
    BinOp {
        op: Box<AstNode>,
        lhs: Box<AstNode>,
        rhs: Box<AstNode>,
    },
    Assign {
        id: String,
        exp: Box<AstNode>,
    },
    IndexAssign {
        id: String,
        indices: Vec<AstNode>,
        exp: Box<AstNode>,
    },
    Op {
        name: String,
        start: usize,
        source_id: usize,
    },
    Id {
        name: String,
        start: usize,
        source_id: usize,
    },
    Call {
        f: Box<AstNode>,
        args: Vec<AstNode>,
        start: usize,
        source_id: usize,
    },
    If {
        cond: Box<AstNode>,
        stmts: Vec<AstNode>,
    },
    While {
        cond: Box<AstNode>,
        stmts: Vec<AstNode>,
    },
    Try {
        tries: Vec<AstNode>,
        err: String,
        catches: Vec<AstNode>,
    },
    Return(Box<AstNode>),
    Raise {
        exp: Box<AstNode>,
        start: usize,
        source_id: usize,
    },
    Dataframe {
        exps: Vec<AstNode>,
        start: usize,
        source_id: usize,
    },
    Matrix(Vec<AstNode>),
    Dict {
        keys: Vec<String>,
        values: Vec<AstNode>,
    },
    List(Vec<AstNode>),
    Series {
        name: String,
        exp: Box<AstNode>,
    },
    Sql {
        op: String,
        from: Box<AstNode>,
        filters: Vec<AstNode>,
        group_type: String,
        groups: Vec<AstNode>,
        ops: Vec<AstNode>,
        sorts: Vec<AstNode>,
        take: Box<AstNode>,
        source_id: usize,
        start: usize,
    },
    SqlBracket(Vec<AstNode>),
    Skip,
}

impl AstNode {
    pub fn as_j(self) -> JResult<J> {
        if let AstNode::J(j) = self {
            Ok(j)
        } else {
            Err(JError::ParserErr("Failed to cast to J".to_owned()))
        }
    }
}
