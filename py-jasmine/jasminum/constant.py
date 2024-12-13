import polars as pl

PL_DATA_TYPE = {
    "bool": pl.Boolean,
    "u8": pl.UInt8,
    "i8": pl.Int8,
    "u16": pl.UInt16,
    "i16": pl.Int16,
    "u32": pl.UInt32,
    "i32": pl.Int32,
    "u64": pl.UInt64,
    "i64": pl.Int64,
    "f32": pl.Float32,
    "f64": pl.Float64,
    "date": pl.Date,
    "datetime": pl.Datetime("ms"),
    "timestamp": pl.Datetime("ns"),
    "duration": pl.Duration("ns"),
    "time": pl.Time,
    "string": pl.String,
    "cat": pl.Categorical,
}
