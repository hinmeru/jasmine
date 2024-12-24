# IPC

## Listen on port

```bash
jasuminum -p 12345
```

## Built-in functions

### hopen

```
hopen("jasmine://user:password:localhost:port")
hopen(`jasmine://user:password:localhost:port)

hopen("duckdb://path/to/db")
hopen(`duckdb://path/to/db)
```

### hclose

```
hclose(handle_id)
```

### hsync

```
hsync(handle_id, query)
```

The allowed query is different for different connection type:

- jasmine
  - string
  - cat
  - [string, arg1, arg2, ...]
- duckdb
  - string

### hasyn

```
hasync(handle_id, query)
```

- jasmine
  - string
  - cat
  - [string, arg1, arg2, ...]

### handle

```
handle()
```

sample output:

```
shape: (2, 4)
┌───────────┬───────────┬───────────┬──────┐
│ handle_id ┆ conn_type ┆ host      ┆ port │
│ ---       ┆ ---       ┆ ---       ┆ ---  │
│ i64       ┆ str       ┆ str       ┆ i64  │
╞═══════════╪═══════════╪═══════════╪══════╡
│ 3         ┆ jasmine   ┆ localhost ┆ 1800 │
│ 4         ┆ duckdb    ┆           ┆ 0    │
└───────────┴───────────┴───────────┴──────┘
```
