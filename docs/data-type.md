# Data Type

## Scalar

| name      | examples                             |
| --------- | ------------------------------------ |
| Null      | 0n, null                             |
| BOOLEAN   | 1b, 0b, true, false                  |
| INT       | 42                                   |
| DATE      | YYYY-MM-DD                           |
| TIME      | HH:mm:ss.sss                         |
| DATETIME  | YYYY-MM-DD[T]HH:mm:ss.sss            |
| TIMESTAMP | YYYY-MM-DD[D]HH:mm:ss.sssssssss      |
| DURATION  | 00000[D]HH:mm:ss.sss,1ns,1s,1m,1h,1D |
| FLOAT     | 4.2, inf, -inf                       |
| STRING    | "string"                             |
| CAT       | `cat, 'cat'                          |

`datetime` and `timestamp` are with timezone information. To convert a timezone

- `` t ~tz.replace `Asia/Tokyo ``
- `` tz.replace(t, `Asia/Tokyo) ``

## List(Mixed Data Types)

```
[1, null, `cat]
```

## Series

| name      | data type      |
| --------- | -------------- |
| bool      | Boolean        |
| f32       | Float32        |
| f64       | Float64        |
| i8        | Int8           |
| i16       | Int16          |
| i32       | Int32          |
| i64       | Int64          |
| u8        | UInt8          |
| u16       | UInt16         |
| u32       | UInt32         |
| u64       | UInt64         |
| date      | Date           |
| datetime  | Datetime("ms") |
| timestamp | Datetime("ns") |
| duration  | Duration       |
| time      | Time           |
| string    | String         |
| cat       | Categorical    |
| list      | List           |
| unknown   | Unknown        |

```
// empty series
`i8$[]

// non-empty series
// i64
1 0n -1

// i8
0i8 1 2

// u8
0n 1u8 3

// bool
1b 0b 1b 0n

// cats
`a`b`c
```

## Dataframe

a collection of series

```
// empty series
df[series1= `i32$[], series2= `f32$[]]


// non-empty series
df[series1 = `i32$0n 0n 0n, series2 = `f32$0n 2.0 3.0]
df[series1 = 0i32 0n 0n, series2 = 0n 2.0 3.0]
```

## Matrix (not yet implemented)

a 2d float array

```
// empty matrix
x[[], [], []]

// non-empty matrix
x[[1, 2], [2, 3], [4, null]]
```

## Dictionary

```
// empty map
{}

// non-empty map
{a:1, b:2, c:3}
// not yet implemented
dict(`a`b`c, 1 2 3)
```

## Temporal Types for Casting

All possible temporal types for casting:

- `year`, year of date
- `month`, month of date
- `month_start`, start date for each month
- `month_end`, end date for each month
- `weekday`, weekday of date, e.g. 1 for Monday,... 7 for Sunday
- `day`, day number of date, 13 for 2025-01-13
- `dt`, date of datetime or timestamp
- `hour`, hour of date, 13 for 2025-01-13D13:00:00
- `minute`, minute of date, 1 for 2025-01-13D13:01:00
- `second`, second of date, 13 for 2025-01-13D13:00:13
- `ms`, millisecond of date, 13 for 2025-01-13T13:00:00.013
- `ns`, nanosecond of date, 13 for 2025-01-13D13:00:00.000000013
