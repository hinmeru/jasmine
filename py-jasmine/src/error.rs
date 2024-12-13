use jasmine::errors::JError;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::PyErr;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JasmineErr {
    #[error(transparent)]
    JasmineError(#[from] JError),

    #[error("{0:?}")]
    PythonError(String),
}

impl std::convert::From<JasmineErr> for PyErr {
    fn from(err: JasmineErr) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));
        use JasmineErr::*;
        match &err {
            JasmineError(e) => match e {
                JError::ParserErr(_) => PyJasmineParseErr::new_err(err.to_string()),
                _ => PyJasmineErr::new_err(err.to_string()),
            },

            PythonError(_) => default(),
        }
    }
}

create_exception!(kola.exceptions, PyJasmineErr, PyException);
create_exception!(kola.exceptions, PyJasmineParseErr, PyException);
