# SQL

## Basic Syntax

```
[select | update | delete] series1, series2, ...
  [
    by series1, series2, ...
    | dyn unit, series1, series2, ...
    | rolling unit, series1, series2, ...
  ]
  from table
  [ where condition1, condition2, ...]
  [ sort {series1, -series2, ...} ]
  [ take number ]
```

### Group By

#### by

```jasmine
t = df[
  sym=100?`a`b`c,
  date=2024-12-01 .. 2025-03-10,
  qty=100?10,
  price=100?1.0,
];

// sum by start of month, hence sum of each month
select sum qty by sym, `month_start$date from t sort sym, date;

// last record in each group
select by sym from t;
```

> [Temporal Types for Casting](data-type.md#temporal-types-for-casting)

> Note: `bar` is pending implementation.

#### dyn

```
// Similar to xbar
// https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.group_by_dynamic.html
select sum qty dyn 10D, date, sym from t;
```

All possible temporal units for `dyn`:

- `1ns` (1 nanosecond)
- `1s` (1 second)
- `1m` (1 minute)
- `1h` (1 hour)
- `1D` (1 calendar day)
- `"1us"` (1 microsecond)
- `"1ms"` (1 millisecond)
- `"1d"` (1 calendar day)
- `"1w"` (1 calendar week)
- `"1mo"` (1 calendar month)
- `"1q"` (1 calendar quarter)
- `"1y"` (1 calendar year)
- `"1i"` (1 index count)

> Note: some units have to be quoted.

#### rolling

```
// a new group by provided by polars
// https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.rolling.html
select sum qty rolling 10D, date, sym from t sort sym, date;
```

## Functional Query

```jasmine
sel(dataframe, wheres, groups, columns)
upd(dataframe, wheres, groups, columns)
del(dataframe, wheres, columns)
```

```jasmine
t = df[
  sym=100?`a`b`c,
  date=2024-12-01 .. 2025-03-10,
  qty=100?10,
  price=100?1.0,
];

// sum by start of month, hence sum of each month
sel(t, null, [col(`sym), `month_start$col(`date)], [sum col(`qty)]);

// last record in each group
sel(t, null, [col(`sym)], null);

// delete qty column
del(t, null, [col(`qty)]);

// delete sym==`a`
del(t, [col(`sym)==`a], null);

// update qty to 100
upd(t, null, null, [lit(100) ~alias `qty]);

// filter sym==`a and update qty to 100
upd(t, [col(`sym)==`a], null, [lit(100) ~alias `qty]);
```
