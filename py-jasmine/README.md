# jasminum

an implementation of the data analytics programming language jasmine powered by [Polars](https://pola.rs/)

## Installation

```
pip install jasminum
```

## Start a jasminum Process

`jasminum`

## Data Type

### Scalar

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

- `` t ~tz `Asia/Hong_Kong ``
- `` tz(t, `Asia/Hong_Kong) ``

### List(Mixed Data Types)

```
[1, null, `cat]
```

### Series

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

### Dataframe

a collection of series

```
// empty series
df[series1= `i32$[], series2= `f32$[]]


// non-empty series
df[series1 = `i32$0n 0n 0n, series2 = `f32$0n 2.0 3.0]
df[series1 = 0i32 0n 0n, series2 = 0n 2.0 3.0]
```

### Matrix (not yet implemented)

a 2d float array

```
// empty matrix
x[[], [], []]

// non-empty matrix
x[[1, 2], [2, 3], [4, null]]
```

### Dictionary

```
// empty map
{}

// non-empty map
{a:1, b:2, c:3}
// not yet implemented
dict(`a`b`c, 1 2 3)
```

### Variable Name

- Starts with alphabets, the var name can include alphabets, number and "\_"
- Starts with CJK character, the var name can include CJK character, alphabets, number and "\_"

### Control Flow

```
if(condition) {
  statement1;
  statement2;
}

while(condition) {
  statement1;
  statement2;
}
```

### Error Handling

```
try {
  statement1;
  statement2;
} catch (err) {
  statement1;
  statement2;
}

```

### Function

```
fn(param1, param2, ...){
  statement1;
  statement2;
  return statement3;
  raise "err";
}
```

### Function Call

```
fn(arg1, arg2, ...)
```

### Functional Call with Parted Args

fn2 is a function requires 2 arguments

```
fn2(arg1)
fn2(, arg2)
```

## Expression

### SQL

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

// functional query
// select
sel(dataframe, exprs, exprs, exprs)
// update
upd(dataframe, exprs, exprs, exprs)
// delete, no group by parameter
del(dataframe, exprs, exprs)
```

### Assignment

```
var1 = exp1
```

### Unary Operation

```
var1 var2
var1(var2)
```

### Binary Operation

```
var1 ~fn0 var2
fn0(var1, var2)
```

### Iteration

#### Each

```
var1 ~each series1
var1 ~each list1
var1 ~each dataframe1

// apply each for 1st param
f2(var1) ~each var2
// apply each for 2nd param
f2(,var2) ~each var1
```
