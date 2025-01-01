# Partitioned Dataframe

## Built-in functions

### wpart

```
wpart(db_path, partition, table, df, sort_series, rechunk, overwrite)
```

- db_path: string
- partition: int|date|null
- table: string
- df: dataframe
- sort_series: cats
- rechunk: bool
- overwrite: bool

```
df0 = df[date=100?[2024-12-09],sym=100?`a`b`c, qty=100?100, price=100?1.0];
wpart("tmp", 2024-12-09, "trade", df0, `sym, 0b, 1b);

df0 = df[date=100?[2024-12-10],sym=100?`a`b`c, qty=100?100, price=100?1.0];
wpart("tmp", 2024-12-10, "trade", df0, `sym, 0b, 1b);
```

### load

load partitioned dataframe

```
load "tmp";

select from t where date==2024-12-29;

select from t where date ~between 2024-12-29 2024-12-31;
```
