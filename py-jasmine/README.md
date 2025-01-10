# jasminum

an implementation of the data analytics programming language jasmine powered by [Polars](https://pola.rs/)

## Installation

```
pip install jasminum
```

## Start a jasminum Process

`jasminum`

## Data Type

[Data Type](https://github.com/hinmeru/jasmine/blob/main/docs/data-type.md)

## Variable Name

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
