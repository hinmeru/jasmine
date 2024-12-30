# IPC

## Listen on port

```bash
jasminum -p 12345
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
  - [string|cat|fn, arg1, arg2, ...]
- duckdb
  - string

### hasyn

```
hasync(handle_id, query)
```

- jasmine
  - string
  - cat
  - [string|cat|fn, arg1, arg2, ...]

### handle

```
handle()
```

sample output:

```
shape: (3, 4)
┌───────────┬───────────┬───────────┬──────┐
│ handle_id ┆ conn_type ┆ host      ┆ port │
│ ---       ┆ ---       ┆ ---       ┆ ---  │
│ i64       ┆ str       ┆ str       ┆ i64  │
╞═══════════╪═══════════╪═══════════╪══════╡
│ 3         ┆ jasmine   ┆ localhost ┆ 1800 │
│ 4         ┆ duckdb    ┆           ┆ 0    │
└───────────┴───────────┴───────────┴──────┘
```

## Message Format

- 1st digit: 1 - little endian
- 2nd digit: 0 - async, 1 - sync, 2 - response
- 3rd/4th digit: 0 - reserved
- 5-8th digit: message byte length count from next byte

### Data Type

| J Type    | Type Value | Fixed Byte Size | Various Byte Size                                                                                                                                                 |
| --------- | ---------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| NULL      | 0          | 0               | -                                                                                                                                                                 |
| BOOLEAN   | 1          | 1               | -                                                                                                                                                                 |
| INT       | 2          | 8               | -                                                                                                                                                                 |
| DATE      | 3          | 4               | -                                                                                                                                                                 |
| TIME      | 4          | 8               | -                                                                                                                                                                 |
| DATETIME  | 5          | 8               | -                                                                                                                                                                 |
| DURATION  | 7          | 8               | -                                                                                                                                                                 |
| FLOAT     | 8          | 8               | -                                                                                                                                                                 |
| STRING    | 9          | -               | 4 bytes length + utf-8 encoded string                                                                                                                             |
| CAT       | 10         | -               | 4 bytes length + utf-8 encoded string                                                                                                                             |
| SERIES    | 11         | -               | 4 bytes length + Arrow IPC serialized bytes                                                                                                                       |
| MATRIX    | -          | -               |                                                                                                                                                                   |
| LIST      | 13         | -               | 4 bytes length + 4 bytes item length + each item bytes                                                                                                            |
| DICT      | 14         | -               | 4 bytes length + 4 bytes item length + <br> 4 bytes keys length + offsets(4 \* item length) + all keys bytes + <br> 4 bytes values length + each value item bytes |
| DATAFRAME | 15         | -               | 4 bytes length + Arrow IPC serialized bytes                                                                                                                       |
| ERR       | 16         | -               | 4 bytes length + utf-8 encoded err string                                                                                                                         |
