use polars::{
    error::{polars_bail, PolarsResult},
    prelude::{Column, DataType, IntoColumn, RoundSeries, TimeUnit},
};

pub fn bar(s0: Column, s1: Column) -> PolarsResult<Option<Column>> {
    let err = || polars_bail!(InvalidOperation: format!("'bar' requires numeric/temporal bar size and series, got '{}' and '{}'", s0.dtype(), s1.dtype()));

    if (s0.dtype().is_primitive_numeric() || s0.dtype().is_temporal())
        && (s1.dtype().is_primitive_numeric() || s1.dtype().is_temporal())
    {
        let s1 = if s0.dtype().is_float() && !s1.dtype().is_float() {
            s1.cast(s0.dtype()).unwrap()
        } else {
            s1.clone()
        };

        let out = match s1.dtype() {
            DataType::Float32 | DataType::Float64 => {
                let bar_size = s0.cast(s1.dtype())?;
                ((s1 / bar_size.clone())?
                    .take_materialized_series()
                    .floor()?
                    .into_column()
                    * bar_size)?
            }
            DataType::Date => {
                let bar_size = s0.cast(&DataType::Int32)?;
                let s1 = s1.cast(&DataType::Int32).unwrap();
                ((s1 / bar_size.clone())? * bar_size)?.cast(&DataType::Date)?
            }
            DataType::Datetime(TimeUnit::Milliseconds, _) => {
                let bar_size = if s0.dtype().eq(&DataType::Duration(TimeUnit::Nanoseconds))
                    || s0.dtype().eq(&DataType::Time)
                {
                    s0.cast(&DataType::Int64).unwrap() / 1000000
                } else {
                    s0.cast(&DataType::Int64).unwrap()
                };
                ((s1.cast(&DataType::Int64).unwrap() / bar_size.clone())? * bar_size)?
                    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?
            }
            DataType::Time
            | DataType::Datetime(TimeUnit::Nanoseconds, _)
            | DataType::Duration(TimeUnit::Nanoseconds) => {
                let bar_size = s0.cast(&DataType::Int64)?;
                ((s1.cast(&DataType::Int64).unwrap() / bar_size.clone())? * bar_size)?
                    .cast(s1.dtype())?
            }
            _ => {
                let bar_size = s0.cast(s1.dtype())?;
                ((s1 / bar_size.clone())? * bar_size)?
            }
        };
        Ok(Some(out))
    } else {
        err()
    }
}
