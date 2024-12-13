use chrono::{DateTime, NaiveDate};
use chrono_tz::Tz;
use jasmine::{j::J, UNIX_EPOCH_DAY};
use numpy::ToPyArray;
use pyo3::{
    pyclass, pymethods,
    types::{PyDict, PyDictMethods, PyTuple},
    IntoPy, PyObject, PyResult, Python, ToPyObject,
};
use pyo3_polars::{PyDataFrame, PySeries};

use crate::error::PyJasmineErr;

#[pyclass]
pub struct JObj {
    j: J,
    #[pyo3(get)]
    j_type: u8,
}

#[pymethods]
impl JObj {
    pub fn as_series(&self) -> PyResult<PySeries> {
        self.j
            .into_series()
            .map_err(|e| PyJasmineErr::new_err(e))
            .map(|s| PySeries(s))
    }

    pub fn tz(&self) -> PyResult<&str> {
        match &self.j {
            J::Datetime { ms: _, timezone } => Ok(timezone),
            J::Timestamp { ns: _, timezone } => Ok(timezone),
            _ => Err(PyJasmineErr::new_err(format!(
                "timezone only available for 'datetime' and 'timestamp', got {}",
                self.j.get_type_name()
            ))),
        }
    }

    #[new]
    fn new_temporal(num: i64, timezone: &str, unit: &str) -> Self {
        if unit == "ms" {
            Self {
                j: J::Datetime {
                    ms: num,
                    timezone: timezone.to_owned(),
                },
                j_type: JType::Datetime as u8,
            }
        } else {
            Self {
                j: J::Timestamp {
                    ns: num,
                    timezone: timezone.to_owned(),
                },
                j_type: JType::Datetime as u8,
            }
        }
    }

    pub fn format_temporal(&self) -> PyResult<String> {
        match &self.j {
            J::Datetime { ms, timezone } => {
                let tz: Tz = timezone.parse().unwrap();
                Ok(DateTime::from_timestamp_millis(*ms)
                    .unwrap()
                    .with_timezone(&tz)
                    .to_string())
            }
            J::Timestamp { ns, timezone } => {
                let tz = timezone.parse::<Tz>().unwrap();
                Ok(DateTime::from_timestamp_nanos(*ns)
                    .with_timezone(&tz)
                    .to_string())
            }
            _ => Err(PyJasmineErr::new_err(format!(
                "format temporal only available for 'datetime' and 'timestamp', got {}",
                self.j.get_type_name()
            ))),
        }
    }

    pub fn with_timezone(&self, timezone: &str) -> PyResult<JObj> {
        timezone
            .parse::<Tz>()
            .map_err(|e| PyJasmineErr::new_err(e.to_string()))
            .unwrap();

        match &self.j {
            J::Datetime { ms, timezone: _ } => Ok(JObj {
                j: J::Datetime {
                    ms: *ms,
                    timezone: timezone.to_string(),
                },
                j_type: self.j_type,
            }),
            J::Timestamp { ns, timezone: _ } => Ok(JObj {
                j: J::Timestamp {
                    ns: *ns,
                    timezone: timezone.to_string(),
                },
                j_type: self.j_type,
            }),
            _ => Err(PyJasmineErr::new_err(format!(
                "with timezone only available for 'datetime' and 'timestamp', got {}",
                self.j.get_type_name()
            ))),
        }
    }

    pub fn as_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.j {
            J::Boolean(v) => Ok(v.into_py(py)),
            J::I64(v) => Ok(v.into_py(py)),
            J::Date(v) => Ok(NaiveDate::from_num_days_from_ce_opt(*v + UNIX_EPOCH_DAY)
                .unwrap()
                .to_object(py)),
            J::Time(v) => Ok(v.into_py(py)),
            J::Datetime { ms, timezone: _ } => Ok(ms.into_py(py)),
            J::Timestamp { ns, timezone: _ } => Ok(ns.into_py(py)),
            J::Duration(v) => Ok(v.into_py(py)),
            J::F64(v) => Ok(v.into_py(py)),
            J::String(v) => Ok(v.into_py(py)),
            J::Cat(v) => Ok(v.into_py(py)),
            J::None => Ok(().to_object(py)),
            J::Series(series) => Ok(PySeries(series.clone()).into_py(py)),
            J::Matrix(matrix) => Ok(matrix.to_pyarray_bound(py).into()),
            J::MixedList(l) => {
                let py_objects = l
                    .into_iter()
                    .map(|k| JObj::new(k.clone()).as_py(py))
                    .collect::<PyResult<Vec<PyObject>>>()?;
                Ok(PyTuple::new_bound(py, py_objects).into())
            }
            J::Dict(dict) => {
                let py_dict = PyDict::new_bound(py);
                for (k, v) in dict.into_iter() {
                    py_dict.set_item(k, JObj::new(v.clone()).as_py(py)?)?;
                }
                Ok(py_dict.into())
            }
            J::DataFrame(data_frame) => Ok(PyDataFrame(data_frame.clone()).into_py(py)),
            J::Err(v) => Err(PyJasmineErr::new_err(v.to_string()).into()),
        }
    }
}

impl JObj {
    pub fn new(j: J) -> Self {
        let j_type = match j {
            J::None => JType::None,
            J::Boolean(_) => JType::Boolean,
            J::I64(_) => JType::I64,
            J::Date(_) => JType::Date,
            J::Time(_) => JType::Time,
            J::Datetime { .. } => JType::Datetime,
            J::Timestamp { .. } => JType::Timestamp,
            J::Duration(_) => JType::Duration,
            J::F64(_) => JType::F64,
            J::String(_) => JType::String,
            J::Cat(_) => JType::Cat,
            J::Series(_) => JType::Series,
            J::Matrix(_) => JType::Matrix,
            J::MixedList(_) => JType::List,
            J::Dict(_) => JType::Dict,
            J::DataFrame(_) => JType::DataFrame,
            J::Err(_) => JType::Err,
        };
        Self {
            j,
            j_type: j_type as u8,
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum JType {
    None,
    Boolean,
    I64,
    Date,
    Time,
    Datetime,
    Timestamp,
    Duration,
    F64,
    String,
    Cat,
    Series,
    Matrix,
    List,
    Dict,
    DataFrame,
    Err,
}
