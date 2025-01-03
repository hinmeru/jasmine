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

#### dyn

```
// Similar to xbar
// https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.group_by_dynamic.html
select sum qty dyn 10D, date, sym from t;
```

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
