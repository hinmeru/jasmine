use jasmine::{JParser, Rule};
use pest::Parser;

use crate::util::pretty_format_rules;

#[path = "./util.rs"]
mod util;

#[test]
fn parse_comments() {
    let code = "/*
    block of comment
*/

\"string❤️\"; // comment

// comment
\"string\";

/* */
    ";
    let pairs = JParser::parse(Rule::Program, code).unwrap();
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            // "COMMENT -> blockComment",
            "Exp -> String",
            // "COMMENT -> lineComment",
            // "COMMENT -> lineComment",
            "Exp -> String",
            // "COMMENT -> blockComment",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case00() {
    let code = "total = sum 1.0 2.0 * 3";
    let pairs = JParser::parse(Rule::Program, code).unwrap();
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> UnaryExp",
            "       -> Id",
            "       -> BinaryExp",
            "         -> Series",
            "           -> Scalar",
            "           -> Scalar",
            "         -> BinaryOp",
            "         -> Exp -> Integer",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case01() {
    let code = "
    f = fn(x,y,z){x + y * z};
    r = f(1, 2, 3);
    g = f(1, , 9);
    h = fn(){9};
    g 3
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Fn",
            "       -> Params",
            "         -> Id",
            "         -> Id",
            "         -> Id",
            "       -> Statements -> Exp -> BinaryExp",
            "             -> Id",
            "             -> BinaryOp",
            "             -> Exp -> BinaryExp",
            "                 -> Id",
            "                 -> BinaryOp",
            "                 -> Exp -> Id",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> FnCall",
            "       -> Id",
            "       -> Arg -> Exp -> Integer",
            "       -> Arg -> Exp -> Integer",
            "       -> Arg -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> FnCall",
            "       -> Id",
            "       -> Arg -> Exp -> Integer",
            "       -> Arg -> Skip",
            "       -> Arg -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Fn",
            "       -> Params",
            "       -> Statements -> Exp -> Integer",
            "Exp -> UnaryExp",
            "   -> Id",
            "   -> Integer",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case02() {
    let code = "
    qty = 7i16 8 9;
    df0 = df[sym = `a`b`b, col1 = 1 2 3, col2 = 1.0 2.0 3.0, 4 5 6, qty];
    df0 = select sum col1+col2, newCol=col2 from t where sym==`a;
    select from df0 where series2  ~between [2.0, 2.2];
    select wmean(qty, price) by sym from df0;
    count df0
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Series",
            "       -> Scalar",
            "       -> Scalar",
            "       -> Scalar",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Dataframe",
            "       -> SeriesExp -> RenameSeriesExp",
            "           -> SeriesName",
            "           -> Cats",
            "       -> SeriesExp -> RenameSeriesExp",
            "           -> SeriesName",
            "           -> Series",
            "             -> Scalar",
            "             -> Scalar",
            "             -> Scalar",
            "       -> SeriesExp -> RenameSeriesExp",
            "           -> SeriesName",
            "           -> Series",
            "             -> Scalar",
            "             -> Scalar",
            "             -> Scalar",
            "       -> SeriesExp -> Series",
            "           -> Scalar",
            "           -> Scalar",
            "           -> Scalar",
            "       -> SeriesExp -> Id",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> SqlExp",
            "       -> SelectOp",
            "         -> SeriesExp -> UnarySqlExp",
            "             -> Id",
            "             -> BinarySqlExp",
            "               -> Id",
            "               -> BinaryOp",
            "               -> Id",
            "         -> SeriesExp -> RenameSeriesExp",
            "             -> SeriesName",
            "             -> Id",
            "       -> FromExp -> Id",
            "       -> FilterExp -> BinarySqlExp",
            "           -> Id",
            "           -> BinaryOp",
            "           -> Cat",
            "Exp -> SqlExp",
            "   -> SelectOp",
            "   -> FromExp -> Id",
            "   -> FilterExp -> BinarySqlExp",
            "       -> Id",
            "       -> BinaryId",
            "       -> List",
            "         -> Exp -> Decimal",
            "         -> Exp -> Decimal",
            "Exp -> SqlExp",
            "   -> SelectOp -> SeriesExp -> FnCall",
            "         -> Id",
            "         -> Arg -> Exp -> Id",
            "         -> Arg -> Exp -> Id",
            "   -> GroupExp -> SeriesExp -> Id",
            "   -> FromExp -> Id",
            "Exp -> UnaryExp",
            "   -> Id",
            "   -> Id",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case02_01() {
    let code = "
    select from df0 where col!=1 sort col1,-col2 take 10;
    delete col1, col2, col3 from df0;
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> SqlExp",
            "   -> SelectOp",
            "   -> FromExp -> Id",
            "   -> FilterExp -> BinarySqlExp",
            "       -> Id",
            "       -> BinaryOp",
            "       -> Integer",
            "   -> SortOp",
            "     -> SortName",
            "     -> SortName",
            "   -> TakeOp -> Exp -> Integer",
            "Exp -> SqlExp",
            "   -> DeleteOp",
            "     -> SeriesName",
            "     -> SeriesName",
            "     -> SeriesName",
            "   -> FromExp -> Id",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case03() {
    let code = "
    r1 = eval [*, 9, 9];
    f = fn(x, y){ x - y};
    r2 = eval([`f, 9, 1]);
    t = timeit([+, 1, 1], 1000);
    t
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> UnaryExp",
            "       -> Id",
            "       -> List",
            "         -> BinaryOp",
            "         -> Exp -> Integer",
            "         -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Fn",
            "       -> Params",
            "         -> Id",
            "         -> Id",
            "       -> Statements -> Exp -> BinaryExp",
            "             -> Id",
            "             -> BinaryOp",
            "             -> Exp -> Id",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> FnCall",
            "       -> Id",
            "       -> Arg -> Exp -> List",
            "             -> Exp -> Cat",
            "             -> Exp -> Integer",
            "             -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> FnCall",
            "       -> Id",
            "       -> Arg -> Exp -> List",
            "             -> BinaryOp",
            "             -> Exp -> Integer",
            "             -> Exp -> Integer",
            "       -> Arg -> Exp -> Integer",
            "Exp -> Id",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case04() {
    let code = "
    d = {a: 1, b: 2, c: 3,};
    d2 = {a: 4, b: 5, c: 6};
    d(`d) = 9;
    r1 = d2 (`c);
    d3(`c) + sum d(`a`d)
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Dict",
            "       -> KeyValueExp",
            "         -> Id",
            "         -> Exp -> Integer",
            "       -> KeyValueExp",
            "         -> Id",
            "         -> Exp -> Integer",
            "       -> KeyValueExp",
            "         -> Id",
            "         -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Dict",
            "       -> KeyValueExp",
            "         -> Id",
            "         -> Exp -> Integer",
            "       -> KeyValueExp",
            "         -> Id",
            "         -> Exp -> Integer",
            "       -> KeyValueExp",
            "         -> Id",
            "         -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> FnCall",
            "     -> Id",
            "     -> Arg -> Exp -> Cat",
            "   -> Exp -> Integer",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> FnCall",
            "       -> Id",
            "       -> Arg -> Exp -> Cat",
            "Exp -> BinaryExp",
            "   -> FnCall",
            "     -> Id",
            "     -> Arg -> Exp -> Cat",
            "   -> BinaryOp",
            "   -> Exp -> UnaryExp",
            "       -> Id",
            "       -> FnCall",
            "         -> Id",
            "         -> Arg -> Exp -> Cats",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case05() {
    let code = "
    f = fn(data){
        if(date>2020-01-01){
            raise error;
            return date;
            date= date + 1;
        };
        2020-01-01
    };;
    r1 = f 2024-04-01;
    r2 = f 2019-01-01;
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> Fn",
            "       -> Params -> Id",
            "       -> Statements",
            "         -> Exp -> IfExp",
            "             -> BinaryExp",
            "               -> Id",
            "               -> BinaryOp",
            "               -> Exp -> Date",
            "             -> Statements",
            "               -> RaiseExp -> Exp -> Id",
            "               -> ReturnExp -> Exp -> Id",
            "               -> Exp -> AssignmentExp",
            "                   -> Id",
            "                   -> Exp -> BinaryExp",
            "                       -> Id",
            "                       -> BinaryOp",
            "                       -> Exp -> Integer",
            "         -> Exp -> Date",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> UnaryExp",
            "       -> Id",
            "       -> Date",
            "Exp -> AssignmentExp",
            "   -> Id",
            "   -> Exp -> UnaryExp",
            "       -> Id",
            "       -> Date",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case06() {
    let code = "
    nest(fn(x){x++sum -2#x}, [1,1], 10)
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> FnCall",
            "   -> Id",
            "   -> Arg -> Exp -> Fn",
            "         -> Params -> Id",
            "         -> Statements -> Exp -> BinaryExp",
            "               -> Id",
            "               -> BinaryOp",
            "               -> Exp -> UnaryExp",
            "                   -> Id",
            "                   -> BinaryExp",
            "                     -> Integer",
            "                     -> BinaryOp",
            "                     -> Exp -> Id",
            "   -> Arg -> Exp -> List",
            "         -> Exp -> Integer",
            "         -> Exp -> Integer",
            "   -> Arg -> Exp -> Integer",
            "EOI",
            ""
        ],
        actual
    )
}

#[test]
fn parse_case07() {
    let code = "
    try {
        a = 1 + `a;
    } catch(err) {
        err == \"type\";
    }
    ";
    let pairs = match JParser::parse(Rule::Program, code) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{}", e);
            panic!("failed to parse")
        }
    };
    let binding = pretty_format_rules(pairs);
    let actual: Vec<&str> = binding.split("\n").collect();
    assert_eq!(
        vec![
            "Exp -> TryExp",
            "   -> Statements -> Exp -> AssignmentExp",
            "         -> Id",
            "         -> Exp -> BinaryExp",
            "             -> Integer",
            "             -> BinaryOp",
            "             -> Exp -> Cat",
            "   -> Id",
            "   -> Statements -> Exp -> BinaryExp",
            "         -> Id",
            "         -> BinaryOp",
            "         -> Exp -> String",
            "EOI",
            ""
        ],
        actual
    )
}
