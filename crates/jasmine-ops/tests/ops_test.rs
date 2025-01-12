#[test]
fn test_binary() {
    use jasmine_ops::bar;
    use polars::prelude::*;

    let s0 = Series::new("bar_size".into(), &[5.0f64]);
    let s1 = Series::new("values".into(), &[1.2f64, 3.7f64, 5.1f64, 8.9f64]);

    let result = bar(s0.into_column(), s1.into_column()).unwrap().unwrap();
    let expected = Series::new("values".into(), &[0.0f64, 0.0f64, 5.0f64, 5.0f64]);

    assert_eq!(result.as_series().unwrap(), &expected);
}
