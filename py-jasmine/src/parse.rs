use jasmine::parse;
use pyo3::{pyfunction, PyResult};

use crate::{ast::Ast, error::PyJasmineParseErr};

#[pyfunction]
pub fn parse_source_code(source_code: &str, source_id: usize) -> PyResult<Vec<Ast>> {
    let ast_nodes =
        parse(source_code, source_id).map_err(|e| PyJasmineParseErr::new_err(e.to_string()))?;

    Ok(ast_nodes
        .into_iter()
        .map(|n| Ast::new(n))
        .collect::<Vec<_>>())
}
