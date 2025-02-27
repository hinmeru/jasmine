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

PL_DTYPE_TO_J_TYPE = {
    pl.Int8: "i8",
    pl.Int16: "i16",
    pl.Int32: "i32",
    pl.Int64: "i64",
    pl.UInt8: "u8",
    pl.UInt16: "u16",
    pl.UInt32: "u32",
    pl.UInt64: "u64",
    pl.Float32: "f32",
    pl.Float64: "f64",
    pl.Boolean: "bool",
    pl.String: "string",
    pl.Date: "date",
    pl.Duration: "duration",
    pl.Time: "time",
    pl.Object: "object",
    pl.Categorical: "cat",
    pl.List: "list",
    pl.Struct: "struct",
}
