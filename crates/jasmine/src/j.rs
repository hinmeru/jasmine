use indexmap::IndexMap;
use ndarray::ArcArray2;
use polars::{
    frame::DataFrame,
    prelude::{CategoricalOrdering, DataType, NamedFrom, TimeUnit},
    series::Series,
};

#[derive(PartialEq, Debug, Clone)]
pub enum J {
    Boolean(bool),                           // -1
    I64(i64),                                // -5
    Date(i32),                               // -6 start from 1970.01.01
    Time(i64),                               // -7 00:00:00.0 - 23:59:59.999999999
    Datetime { ms: i64, timezone: String },  // -8 start from 1970.01.01T00:00:00.0
    Timestamp { ns: i64, timezone: String }, // -9 start from 1970.01.01D00:00:00.0
    Duration(i64),                           // -10
    F64(f64),                                // -12
    String(String),                          // -13
    Cat(String),                             // -14

    None, // 0

    Series(Series), // 1-14 -> Arrow IPC

    Matrix(ArcArray2<f64>), // 21

    MixedList(Vec<J>),         // 90
    Dict(IndexMap<String, J>), // 91 -> skip Dataframe
    DataFrame(DataFrame),      // 92 -> Arrow IPC

    Err(String), // 128 => string
}

impl J {
    pub fn into_series(&self) -> Result<Series, String> {
        match self {
            J::Boolean(s) => Ok(Series::new("".into(), vec![*s])),
            J::I64(s) => Ok(Series::new("".into(), vec![*s])),
            J::F64(s) => Ok(Series::new("".into(), vec![*s])),
            J::Date(s) => Ok(Series::new("".into(), vec![*s])
                .cast(&DataType::Date)
                .unwrap()),
            J::Timestamp { ns, timezone } => Ok(Series::new("".into(), vec![*ns])
                .cast(&DataType::Datetime(
                    TimeUnit::Nanoseconds,
                    Some(timezone.into()),
                ))
                .unwrap()),
            J::Datetime { ms, timezone } => Ok(Series::new("".into(), vec![*ms])
                .cast(&DataType::Datetime(
                    TimeUnit::Milliseconds,
                    Some(timezone.into()),
                ))
                .unwrap()),
            J::Time(s) => Ok(Series::new("".into(), vec![*s])
                .cast(&DataType::Time)
                .unwrap()),
            J::Duration(s) => Ok(Series::new("".into(), vec![*s])
                .cast(&DataType::Duration(TimeUnit::Nanoseconds))
                .unwrap()),
            J::Cat(s) => Ok(Series::new("".into(), vec![s.to_owned()])
                .cast(&DataType::Categorical(None, CategoricalOrdering::Lexical))
                .unwrap()),
            J::String(s) => Ok(Series::new("".into(), vec![s.to_owned()])),
            J::None => Ok(Series::new_null("".into(), 1)),
            _ => Err("cannot turn into a series".to_owned()),
        }
    }

    pub fn series(&self) -> Result<Series, String> {
        match self {
            J::Series(s) => Ok(s.clone()),
            _ => Err("not a series".to_owned()),
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            J::I64(_) | J::F64(_) => true,
            _ => false,
        }
    }

    pub fn is_bool(&self) -> bool {
        match self {
            J::Boolean(_) => true,
            _ => false,
        }
    }

    pub fn get_type_name(&self) -> String {
        match self {
            J::Boolean(_) => "bool".to_owned(),
            J::I64(_) => "i64".to_owned(),
            J::F64(_) => "f64".to_owned(),
            J::Date(_) => "date".to_owned(),
            J::Timestamp { .. } => "timestamp".to_owned(),
            J::Datetime { .. } => "datetime".to_owned(),
            J::Time(_) => "time".to_owned(),
            J::Duration(_) => "duration".to_owned(),
            J::Cat(_) => "sym".to_owned(),
            J::String(_) => "str".to_owned(),

            J::MixedList(_) => "list".to_owned(),
            J::Series(_) => "series".to_owned(),
            J::Matrix(_) => "matrix".to_owned(),
            J::Dict(_) => "dict".to_owned(),
            J::DataFrame(_) => "df".to_owned(),
            J::Err(_) => "err".to_owned(),
            J::None => "none".to_owned(),
        }
    }
}
