use polars::prelude::{CompatLevel, Field, PolarsResult, Series};
use pyo3_polars::derive::polars_expr;

fn bar_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[1];
    Ok(field.clone())
}

#[polars_expr(output_type_func = bar_output)]
pub fn bar(inputs: &[Series]) -> PolarsResult<Series> {
    let s0 = inputs[0].clone();
    let s1 = inputs[1].clone();
    Ok(jasmine_ops::bin::bar(s0.into(), s1.into())?
        .unwrap()
        .as_series()
        .unwrap()
        .clone())
}
