# Quick Started

## Install

```bash
pip install jasminum
```

## Run

```bash
jasminum
```

## Set Max Threads

```bash
export POLARS_MAX_THREADS=6
```

> if not set, the polars will use all available threads.

## for kdb+/q Developer

`jasmine` is meant to follow general programming language syntax, while keeping the `sql` syntax.

1. `[]` => `()`, as in function call
2. `()` => `[]`, as in creating a list
3. `~` to use a function a binary format, like `timezone ~like "Asia"`
4. `{[args]body}` => `fn(args){body}`, as in function definition
5. `if[condition;statement]` => `if(condition){statement}`, as in if statement;
6. `while[condition;statement]` => `while(condition){statement}`, as in while statement;
7. `.Q.trp` => `try{statement}catch(e){statement}`, as in try-catch statement;
8. `//`, and `/* */` are for comments.
9. `~like` uses regular expression.
