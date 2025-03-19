use crate::ast_node::AstNode;
use crate::j::J;
use chrono::{self, TimeZone};
use chrono::{Datelike, Local};
use indexmap::IndexMap;
use pest::error::{Error as PestError, ErrorVariant};
use pest::Span;
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use polars::datatypes::{CategoricalOrdering, DataType as PolarsDataType, Float64Type, TimeUnit};
use polars::frame::DataFrame;
use polars::prelude::{Column, IndexOrder, NamedFrom};
use polars::series::Series;
use regex::bytes::Regex;
use regex::RegexSet;

pub const UNIX_EPOCH_DAY: i32 = 719_163;

pub const NS_IN_DAY: i64 = 86_400_000_000_000;

#[derive(Parser)]
#[grammar = "jasmine.pest"]
pub struct JParser;

fn parse_binary_op(pair: Pair<Rule>, source_id: usize) -> Result<AstNode, PestError<Rule>> {
    match pair.as_rule() {
        Rule::BinaryOp => Ok(AstNode::Op {
            name: pair.as_str().to_owned(),
            start: pair.as_span().start(),
            source_id,
        }),
        Rule::BinaryId => {
            if is_keyword(&pair.as_str()[1..]) {
                Err(raise_error(
                    format!("Keyword cannot be used as identifier: {}", pair.as_str()),
                    pair.as_span(),
                ))
            } else {
                Ok(AstNode::Op {
                    name: pair.as_str()[1..].to_owned(),
                    start: pair.as_span().start(),
                    source_id,
                })
            }
        }
        _ => Err(raise_error(
            format!("Unexpected binary op/function: {}", pair.as_str()),
            pair.as_span(),
        )),
    }
}

fn parse_exp(pair: Pair<Rule>, source_id: usize) -> Result<AstNode, PestError<Rule>> {
    let rule = pair.as_rule();
    match rule {
        Rule::Exp => parse_exp(pair.into_inner().next().unwrap(), source_id),
        Rule::UnaryExp | Rule::UnarySqlExp => {
            let mut pair = pair.into_inner();
            let unary = pair.next().unwrap();
            let exp = pair.next().unwrap();
            let exp = parse_exp(exp, source_id)?;
            Ok(AstNode::UnaryOp {
                op: Box::new(parse_exp(unary, source_id)?),
                exp: Box::new(exp),
            })
        }
        Rule::BinaryExp | Rule::BinarySqlExp => {
            let mut pair = pair.into_inner();
            let lhs_pair = pair.next().unwrap();
            let lhs = parse_exp(lhs_pair, source_id)?;
            let binary_exp = pair.next().unwrap();
            let rhs_pair = pair.next().unwrap();
            let rhs = parse_exp(rhs_pair, source_id)?;
            Ok(AstNode::BinOp {
                op: Box::new(parse_binary_op(binary_exp, source_id)?),
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            })
        }
        Rule::Integer
        | Rule::Boolean
        | Rule::Decimal
        | Rule::Date
        | Rule::Time
        | Rule::Datetime
        | Rule::Timestamp
        | Rule::Duration
        | Rule::CatAlt
        | Rule::Cat
        | Rule::String
        | Rule::Null => parse_j(pair),
        Rule::Series => parse_series(pair),
        Rule::Cats => parse_cats(pair),
        Rule::AssignmentExp => {
            let mut pairs = pair.into_inner();
            let id = pairs.next().unwrap();
            if id.as_rule() == Rule::FnCall {
                let mut fn_call = id.into_inner();
                let id = fn_call.next().unwrap();
                let mut indices: Vec<AstNode> = Vec::with_capacity(fn_call.len() - 1);
                for arg in fn_call {
                    indices.push(parse_exp(arg.into_inner().next().unwrap(), source_id)?)
                }
                let exp = parse_exp(pairs.next().unwrap(), source_id)?;
                Ok(AstNode::IndexAssign {
                    id: Box::new(AstNode::Id {
                        name: id.as_str().to_owned(),
                        start: id.as_span().start(),
                        source_id,
                    }),
                    indices,
                    exp: Box::new(exp),
                })
            } else {
                let exp = pairs.next().unwrap();
                let exp = parse_exp(exp, source_id)?;
                if is_keyword(id.as_str()) {
                    Err(raise_error(
                        format!("Keyword cannot be used as identifier: {}", id.as_str()),
                        id.as_span(),
                    ))
                } else {
                    Ok(AstNode::Assign {
                        id: id.as_str().to_owned(),
                        exp: Box::new(exp),
                    })
                }
            }
        }
        Rule::Id | Rule::BinaryOp | Rule::GlobalId => {
            if is_keyword(pair.as_str()) {
                Err(raise_error(
                    format!("Keyword cannot be used as identifier: {}", pair.as_str()),
                    pair.as_span(),
                ))
            } else {
                Ok(AstNode::Id {
                    name: pair.as_str().to_owned(),
                    start: pair.as_span().start(),
                    source_id,
                })
            }
        }
        Rule::Fn => {
            let fn_body = pair.as_str();
            let fn_span = pair.as_span();
            let mut pairs = pair.into_inner();
            let pair = pairs.next().unwrap();
            let mut inner = pair.into_inner();
            let mut params: Vec<String> = Vec::with_capacity(inner.len());
            while let Some(pair) = inner.next() {
                params.push(pair.as_str().to_owned())
            }
            let pairs = pairs.next().unwrap().into_inner();
            let mut nodes = Vec::with_capacity(pairs.len());
            for pair in pairs {
                nodes.push(parse_exp(pair, source_id)?)
            }
            Ok(AstNode::Fn {
                stmts: nodes,
                arg_names: params,
                fn_body: fn_body.to_owned(),
                start: fn_span.start(),
                source_id,
            })
        }
        Rule::FnCall => {
            let span = pair.as_span();
            let mut pairs = pair.into_inner();
            let f = parse_exp(pairs.next().unwrap(), source_id)?;
            let arg_len = pairs.len();
            let mut args = Vec::with_capacity(arg_len);
            for pair in pairs {
                let arg = pair.into_inner().next().unwrap();
                if arg_len == 1 && arg.as_rule() == Rule::Skip {
                    args = vec![];
                    break;
                }
                args.push(parse_exp(arg, source_id)?)
            }
            // if f is eval, and first args is J::String, parse J::string
            Ok(AstNode::Call {
                f: Box::new(f),
                args,
                start: span.start(),
                source_id,
            })
        }
        Rule::IfExp => {
            let mut pairs = pair.into_inner();
            let cond = parse_exp(pairs.next().unwrap(), source_id)?;
            let mut nodes = Vec::new();
            for pair in pairs.next().unwrap().into_inner() {
                let rule = pair.as_rule();
                nodes.push(parse_exp(pair, source_id)?);
                if rule == Rule::ReturnExp {
                    break;
                }
            }
            Ok(AstNode::If {
                cond: Box::new(cond),
                stmts: nodes,
            })
        }
        Rule::WhileExp => {
            let mut pairs = pair.into_inner();
            let cond = parse_exp(pairs.next().unwrap(), source_id)?;
            let mut nodes = Vec::new();
            for pair in pairs.next().unwrap().into_inner() {
                let rule = pair.as_rule();
                nodes.push(parse_exp(pair, source_id)?);
                if rule == Rule::ReturnExp {
                    break;
                }
            }
            Ok(AstNode::While {
                cond: Box::new(cond),
                stmts: nodes,
            })
        }
        Rule::TryExp => {
            let mut pairs = pair.into_inner();
            let mut tries = Vec::new();
            let mut catches = Vec::new();
            for pair in pairs.next().unwrap().into_inner() {
                tries.push(parse_exp(pair, source_id)?);
            }
            let err = pairs.next().unwrap().as_str().to_owned();
            for pair in pairs.next().unwrap().into_inner() {
                catches.push(parse_exp(pair, source_id)?);
            }
            Ok(AstNode::Try {
                tries,
                err,
                catches,
            })
        }
        Rule::ReturnExp => {
            let node = parse_exp(pair.into_inner().next().unwrap(), source_id)?;
            Ok(AstNode::Return(Box::new(node)))
        }
        Rule::RaiseExp => {
            let start = pair.as_span().start();
            let node = parse_exp(pair.into_inner().next().unwrap(), source_id)?;
            Ok(AstNode::Raise {
                exp: Box::new(node),
                start,
                source_id,
            })
        }
        Rule::Skip => Ok(AstNode::Skip),
        Rule::Dataframe => {
            let span = pair.as_span();
            let cols = pair.into_inner();
            let mut series_exps: Vec<AstNode> = Vec::with_capacity(cols.len());
            let mut all_series = true;
            for (i, col_exp) in cols.enumerate() {
                let name: String;
                let exp: AstNode;
                let node = col_exp.into_inner().next().unwrap();
                if node.as_rule() == Rule::RenameSeriesExp {
                    let mut nodes = node.into_inner();
                    name = nodes.next().unwrap().as_str().to_owned();
                    exp = parse_exp(nodes.next().unwrap(), source_id)?;
                } else {
                    name = format!("series{:02}", i);
                    exp = parse_exp(node, source_id)?
                }
                if let AstNode::J(j) = exp {
                    if let J::Series(mut s) = j {
                        s.rename(name.into());
                        series_exps.push(AstNode::J(J::Series(s)));
                    } else {
                        let mut s = j
                            .into_series()
                            .map_err(|e| raise_error(e.to_string(), span))?;
                        s.rename(name.into());
                        series_exps.push(AstNode::J(J::Series(s)));
                    }
                } else if let AstNode::Id {
                    name,
                    start: _,
                    source_id: _,
                } = &exp
                {
                    series_exps.push(AstNode::Series {
                        name: name.to_owned(),
                        exp: Box::new(exp),
                    });
                    all_series = false;
                } else {
                    series_exps.push(AstNode::Series {
                        name,
                        exp: Box::new(exp),
                    });
                    all_series = false;
                }
            }
            if all_series {
                let series: Vec<Column> = series_exps
                    .into_iter()
                    .map(|node| node.as_j().unwrap().series().unwrap().clone().into())
                    .collect();
                let df = match DataFrame::new(series) {
                    Ok(df) => df,
                    Err(e) => return Err(raise_error(e.to_string(), span)),
                };
                Ok(AstNode::J(J::DataFrame(df)))
            } else {
                Ok(AstNode::Dataframe {
                    exps: series_exps,
                    start: span.start(),
                    source_id,
                })
            }
        }
        Rule::Matrix => {
            let span = pair.as_span();
            let rows = pair.into_inner();
            let mut exps: Vec<AstNode> = Vec::with_capacity(rows.len());
            let mut all_series = true;
            for (i, col_exp) in rows.enumerate() {
                let col_name: String;
                let exp: AstNode;
                let node = col_exp.into_inner().next().unwrap();
                let node_span = node.as_span();
                col_name = format!("col{:02}", i);
                exp = parse_exp(node, source_id)?;
                if let AstNode::J(j) = exp {
                    let type_name = j.get_type_name();
                    if let J::Series(mut s) = j {
                        if !(s.dtype().is_primitive_numeric() || s.dtype().is_bool()) {
                            return Err(raise_error(
                                format!("Requires numeric data type, got '{}'", s.dtype()),
                                node_span,
                            ));
                        }
                        s.rename(col_name.into());
                        exps.push(AstNode::J(J::Series(s)));
                    } else {
                        if !(j.is_numeric() || j.is_bool()) {
                            return Err(raise_error(
                                format!("Requires numeric data type, got '{}'", type_name),
                                node_span,
                            ));
                        }
                        let mut s = j.into_series().unwrap();
                        s.rename(col_name.into());
                        exps.push(AstNode::J(J::Series(s)));
                    }
                } else {
                    exps.push(AstNode::Series {
                        name: col_name,
                        exp: Box::new(exp),
                    });
                    all_series = false;
                }
            }
            if all_series {
                let cols: Vec<Column> = exps
                    .into_iter()
                    .map(|node| node.as_j().unwrap().series().unwrap().clone().into())
                    .collect();
                let df = match DataFrame::new(cols) {
                    Ok(df) => df,
                    Err(e) => return Err(raise_error(e.to_string(), span)),
                };
                let matrix = df
                    .to_ndarray::<Float64Type>(IndexOrder::C)
                    .map_err(|e| raise_error(e.to_string(), span))?;
                Ok(AstNode::J(J::Matrix(matrix.reversed_axes().to_shared())))
            } else {
                Ok(AstNode::Matrix(exps))
            }
        }
        Rule::SqlExp => parse_sql(pair, source_id),
        Rule::BracketExp | Rule::BracketSqlExp => {
            Ok(parse_exp(pair.into_inner().next().unwrap(), source_id)?)
        }
        Rule::List => {
            let pair_clone = pair.clone();
            let pairs = pair.into_inner();
            if pairs.len() == 0 {
                return Ok(AstNode::J(J::MixedList(vec![])));
            }
            let mut list = Vec::with_capacity(pairs.len());
            let mut all_j = true;
            let mut pairs_clone = pairs.clone();
            let first_rule = pairs_clone
                .next()
                .unwrap()
                .into_inner()
                .next()
                .map_or(Rule::Null, |p| p.as_rule());
            let mut all_same_type = true;
            for pair in pairs_clone {
                if first_rule != pair.into_inner().next().map_or(Rule::Null, |p| p.as_rule()) {
                    all_same_type = false;
                }
            }
            if all_same_type {
                match first_rule {
                    Rule::Cat
                    | Rule::CatAlt
                    | Rule::Boolean
                    | Rule::Timestamp
                    | Rule::Datetime
                    | Rule::Duration
                    | Rule::Date
                    | Rule::Time
                    | Rule::Decimal
                    | Rule::String
                    | Rule::Integer => return parse_series(pair_clone),
                    _ => {}
                }
            }
            for pair in pairs {
                let ast = parse_list(pair, source_id)?;
                if let AstNode::J(_) = &ast {
                } else {
                    all_j = false
                }
                list.push(ast)
            }
            if all_j {
                Ok(AstNode::J(J::MixedList(
                    list.into_iter().map(|a| a.as_j().unwrap()).collect(),
                )))
            } else {
                Ok(AstNode::List(list))
            }
        }
        Rule::Dict => {
            let pairs = pair.into_inner();
            let mut keys: Vec<String> = Vec::with_capacity(pairs.len());
            let mut values: Vec<AstNode> = Vec::with_capacity(pairs.len());
            let mut all_j = true;
            for pair in pairs {
                let mut kv = pair.into_inner();
                let key_node = kv.next().unwrap();
                let key = match key_node.as_rule() {
                    Rule::Id => key_node.as_str(),
                    Rule::Cat => &key_node.as_str()[1..],
                    _ => &key_node.as_str()[1..key_node.as_str().len() - 1],
                };
                keys.push(key.to_string());
                let value = parse_exp(kv.next().unwrap(), source_id)?;
                if let AstNode::J(_) = &value {
                } else {
                    all_j = false
                }
                values.push(value);
            }
            if all_j {
                let dict = IndexMap::from_iter(
                    keys.into_iter()
                        .zip(values.into_iter().map(|a| a.as_j().unwrap())),
                );
                Ok(AstNode::J(J::Dict(dict)))
            } else {
                Ok(AstNode::Dict { keys, values })
            }
        }
        unexpected_exp => Err(raise_error(
            format!("Unexpected rule: {:?}", unexpected_exp),
            pair.as_span(),
        )),
    }
}

fn parse_list(pair: Pair<Rule>, source_id: usize) -> Result<AstNode, PestError<Rule>> {
    match pair.as_rule() {
        Rule::BinaryOp => Ok(AstNode::Op {
            name: pair.as_str().to_owned(),
            start: pair.as_span().start(),
            source_id,
        }),
        _ => parse_exp(pair, source_id),
        // _ => Err(raise_error(
        //     format!("Unexpected rule in list expression: {:?}", pair.as_str()),
        //     pair.as_span(),
        // )),
    }
}

macro_rules! impl_parse_num {
    ($fn_name:ident, $ty_str:literal, $ty:ty) => {
        fn $fn_name(unknowns: Vec<&str>, span: Span) -> Result<AstNode, PestError<Rule>> {
            match unknowns
                .iter()
                .map(|s| {
                    let s = if s.ends_with($ty_str) {
                        &s[..s.len() - $ty_str.len()]
                    } else {
                        s
                    };
                    if s.is_empty() || s == "null" || s == "0n" {
                        return Ok(None);
                    } else {
                        return s
                            .parse::<$ty>()
                            .map(|n| Some(n))
                            .map_err(|e| format!("'{}': {}", s, e).to_string());
                    }
                })
                .collect::<Result<Vec<Option<$ty>>, String>>()
            {
                Ok(n) => Ok(AstNode::J(J::Series(Series::new("".into(), n)))),
                Err(e) => Err(raise_error(e.to_string(), span)),
            }
        }
    };
}

impl_parse_num!(parse_u8, "u8", u8);
impl_parse_num!(parse_i8, "i8", i8);
impl_parse_num!(parse_u16, "u16", u16);
impl_parse_num!(parse_i16, "i16", i16);
impl_parse_num!(parse_u32, "u32", u32);
impl_parse_num!(parse_i32, "i32", i32);
impl_parse_num!(parse_u64, "u64", u64);
impl_parse_num!(parse_i64, "i64", i64);
impl_parse_num!(parse_f32, "f32", f32);
impl_parse_num!(parse_f64, "f64", f64);

fn parse_series(pair: Pair<Rule>) -> Result<AstNode, PestError<Rule>> {
    let mut first_scalar = "";
    let span = pair.as_span();
    let unknowns: Vec<&str> = pair.into_inner().map(|p| p.as_str()).collect();
    let len = unknowns.len();
    for scalar in unknowns.iter() {
        if !scalar.is_empty() && *scalar != "null" && *scalar != "0n" {
            first_scalar = scalar;
            break;
        }
    }
    if len == 1 && first_scalar == "" {
        return Ok(AstNode::J(J::Series(Series::new_empty(
            "".into(),
            &PolarsDataType::Null,
        ))));
    }
    let set = RegexSet::new(&[
        r"^(true|false|1b|0b)$",
        r"^\d+u8$",
        r"^-?\d+i8$",
        r"^\d+u16$",
        r"^-?\d+i16$",
        r"^\d+u32$",
        r"^-?\d+i32$",
        r"^\d+u64$",
        r"^-?\d+(i64)?$",
        r"^-?([0-9]+([.][0-9]*)?|[.][0-9]+)f32$",
        r"^-?([0-9]+([.][0-9]*)?|[.][0-9]+)(f64)?$",
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{2}:\d{2}:\d{2}\.\d{0,9}$",
        r"^\d{4}-\d{2}-\d{2}T(\d{2}:\d{2}:\d{2}(\.\d{0,3})?)?$",
        r"^\d{4}-\d{2}-\d{2}D(\d{2}:\d{2}:\d{2}(\.\d{0,9})?)?$",
        r"^-?\d+D(\d{2}:\d{2}:\d{2}(\.\d{0,9})?)?$",
        r"^-?\d+(ns|s|m|h)$",
        r"^'[^']*'$",
        r#"^"[^"]*"$"#,
        r"(^(null|0n)$|^$)",
        r"^`.*$",
    ])
    .unwrap();

    let matches: Vec<_> = set.matches(&first_scalar).into_iter().collect();
    let first_match = matches.first().copied().unwrap_or(set.len());

    match first_match {
        0 => {
            let mut bools = Vec::with_capacity(len);
            for bool in unknowns.iter() {
                match *bool {
                    "true" => bools.push(Some(true)),
                    "1b" => bools.push(Some(true)),
                    "false" => bools.push(Some(false)),
                    "0b" => bools.push(Some(false)),
                    "null" | "0n" | "" => bools.push(None),
                    _ => {
                        return Err(raise_error(
                            format!("unrecognized bool value {}", bool),
                            span,
                        ))
                    }
                }
            }
            let s = Series::new("".into(), bools);
            Ok(AstNode::J(J::Series(s)))
        }
        1 => parse_u8(unknowns, span),
        2 => parse_i8(unknowns, span),
        3 => parse_u16(unknowns, span),
        4 => parse_i16(unknowns, span),
        5 => parse_u32(unknowns, span),
        6 => parse_i32(unknowns, span),
        7 => parse_u64(unknowns, span),
        8 => parse_i64(unknowns, span),
        9 => parse_f32(unknowns, span),
        10 => parse_f64(unknowns, span),
        11 => {
            let dates = unknowns
                .iter()
                .map(|s| {
                    if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        parse_date(s)
                            .map_err(|e| raise_error(format!("'{}': {}", s, e), span))
                            .map(|d| Some(d - UNIX_EPOCH_DAY))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), dates)
                    .cast(&PolarsDataType::Date)
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        12 => {
            let times = unknowns
                .iter()
                .map(|s| {
                    if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        parse_time(*s)
                            .map_err(|e| raise_error(format!("'{}': {}", s, e), span))
                            .map(|t| Some(t))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), times)
                    .cast(&PolarsDataType::Time)
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        13 => {
            let datetimes = unknowns
                .iter()
                .map(|s| {
                    if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        parse_datetime(*s)
                            .map_err(|e| raise_error(format!("'{}': {}", s, e), span))
                            .map(|dt| Some(dt))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), datetimes)
                    .cast(&PolarsDataType::Datetime(
                        TimeUnit::Milliseconds,
                        Some(
                            iana_time_zone::get_timezone()
                                .unwrap_or("UTC".to_owned())
                                .into(),
                        ),
                    ))
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        14 => {
            let timestamps = unknowns
                .iter()
                .map(|s| {
                    if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        parse_timestamp(*s)
                            .map_err(|e| raise_error(format!("'{}': {}", s, e), span))
                            .map(|ts| Some(ts))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), timestamps)
                    .cast(&PolarsDataType::Datetime(
                        TimeUnit::Nanoseconds,
                        Some(
                            iana_time_zone::get_timezone()
                                .unwrap_or("UTC".to_owned())
                                .into(),
                        ),
                    ))
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        15 | 16 => {
            let times = unknowns
                .iter()
                .map(|s| {
                    if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        parse_duration(*s)
                            .map_err(|e| raise_error(format!("'{}': {}", s, e), span))
                            .map(|t| Some(t))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), times)
                    .cast(&PolarsDataType::Duration(TimeUnit::Nanoseconds))
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        17 => {
            let cats = unknowns
                .iter()
                .map(|s| {
                    if Regex::new(r"^'[^']*'$").unwrap().is_match(s.as_bytes()) {
                        Ok(Some(s[1..s.len() - 1].to_owned()))
                    } else if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        Err(raise_error(
                            format!("'{}': {}", s, "not a categorical"),
                            span,
                        ))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), cats)
                    .cast(&PolarsDataType::Categorical(
                        None,
                        CategoricalOrdering::Lexical,
                    ))
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        18 => {
            let strings = unknowns
                .iter()
                .map(|s| {
                    if Regex::new(r#"^"[^"]*"$"#).unwrap().is_match(s.as_bytes()) {
                        Ok(Some(s[1..s.len() - 1].to_owned()))
                    } else if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        Err(raise_error(format!("'{}': {}", s, "not a string"), span))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(Series::new("".into(), strings))))
        }
        19 => Ok(AstNode::J(J::Series(Series::new_null("".into(), len)))),
        20 => {
            let cats = unknowns
                .iter()
                .map(|s| {
                    if Regex::new(r"^`.*$").unwrap().is_match(s.as_bytes()) {
                        Ok(Some(s[1..].to_owned()))
                    } else if *s == "" || *s == "null" || *s == "0n" {
                        Ok(None)
                    } else {
                        Err(raise_error(
                            format!("'{}': {}", s, "not a categorical"),
                            span,
                        ))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AstNode::J(J::Series(
                Series::new("".into(), cats)
                    .cast(&PolarsDataType::Categorical(
                        None,
                        CategoricalOrdering::Lexical,
                    ))
                    .map_err(|e| raise_error(e.to_string(), span))?,
            )))
        }
        _ => Err(raise_error("unknown series".to_owned(), span)),
    }
}

fn parse_j(pair: Pair<Rule>) -> Result<AstNode, PestError<Rule>> {
    match pair.as_rule() {
        Rule::Boolean => Ok(AstNode::J(J::Boolean(
            pair.as_str() == "1b" || pair.as_str() == "true",
        ))),
        Rule::Integer => match pair.as_str().parse::<i64>() {
            Ok(n) => Ok(AstNode::J(J::I64(n))),
            Err(e) => Err(raise_error(e.to_string(), pair.as_span())),
        },
        Rule::Decimal => match pair.as_str().parse::<f64>() {
            Ok(n) => Ok(AstNode::J(J::F64(n))),
            Err(e) => Err(raise_error(e.to_string(), pair.as_span())),
        },
        Rule::Date => {
            let j = parse_date(pair.as_str())
                .map_err(|e| raise_error(e.to_string(), pair.as_span()))
                .map(|j| J::Date(j - UNIX_EPOCH_DAY))?;
            Ok(AstNode::J(j))
        }
        Rule::Time => {
            let j = parse_time(pair.as_str())
                .map_err(|e| raise_error(e.to_string(), pair.as_span()))
                .map(|j| J::Time(j))?;
            Ok(AstNode::J(j))
        }
        Rule::Datetime => {
            let j = parse_datetime(pair.as_str())
                .map_err(|e| raise_error(e.to_string(), pair.as_span()))
                .map(|j| J::Datetime {
                    ms: j,
                    timezone: iana_time_zone::get_timezone().unwrap_or("UTC".to_owned()),
                })?;
            Ok(AstNode::J(j))
        }
        Rule::Timestamp => {
            let j = parse_timestamp(pair.as_str())
                .map_err(|e| raise_error(e.to_string(), pair.as_span()))
                .map(|j| J::Timestamp {
                    ns: j,
                    timezone: iana_time_zone::get_timezone().unwrap_or("UTC".to_owned()),
                })?;
            Ok(AstNode::J(j))
        }
        Rule::Duration => {
            let j = parse_duration(pair.as_str())
                .map_err(|e| raise_error(e.to_string(), pair.as_span()))
                .map(|j| J::Duration(j))?;
            Ok(AstNode::J(j))
        }
        Rule::Cat => Ok(AstNode::J(J::Cat(pair.as_str()[1..].to_string()))),
        Rule::CatAlt => Ok(AstNode::J(J::Cat(
            pair.as_str()[1..pair.as_str().len() - 1].to_string(),
        ))),
        Rule::String => {
            let str = pair.as_str();
            // Strip leading and ending quotes.
            let str = &str[1..str.len() - 1];
            // Escaped string quotes become single quotes here.
            Ok(AstNode::J(J::String(str.to_owned())))
        }
        Rule::Null => Ok(AstNode::J(J::Null)),
        unexpected_exp => Err(raise_error(
            format!("Unexpected j: {:?}", unexpected_exp),
            pair.as_span(),
        )),
    }
}

fn parse_cats(pair: Pair<Rule>) -> Result<AstNode, PestError<Rule>> {
    let cats = pair.as_str()[1..].split("`").collect::<Vec<_>>();
    Ok(AstNode::J(J::Series(
        Series::new("".into(), cats)
            .cast(&PolarsDataType::Categorical(
                None,
                CategoricalOrdering::Lexical,
            ))
            .map_err(|e| raise_error(e.to_string(), pair.as_span()))?,
    )))
}

fn parse_sql(pair: Pair<Rule>, source_id: usize) -> Result<AstNode, PestError<Rule>> {
    let span = pair.as_span();
    let mut pairs = pair.into_inner();
    // select, update, exec, delete
    let mut op = "select";
    let mut ops: Vec<AstNode> = Vec::new();
    let mut groups: Vec<AstNode> = Vec::new();
    let mut from: AstNode = AstNode::Skip;
    let mut filters: Vec<AstNode> = Vec::new();
    let mut sorts: Vec<AstNode> = Vec::new();
    let mut take = AstNode::J(J::Null);
    let mut group_type = "by";
    while let Some(some_pair) = pairs.next() {
        match some_pair.as_rule() {
            Rule::SelectOp | Rule::UpdateOp | Rule::DeleteOp => {
                op = &some_pair.as_str()[..6];
                let op_pairs = some_pair.into_inner();
                for op_pair in op_pairs {
                    ops.push(parse_sql_col_exp(op_pair, source_id)?)
                }
            }
            Rule::GroupExp => {
                group_type = match &some_pair.as_str()[0..1] {
                    "d" => "dyn",
                    "r" => "rolling",
                    _ => "by",
                };
                let group_pairs = some_pair.into_inner();
                groups = Vec::with_capacity(group_pairs.len());
                for group_pair in group_pairs {
                    groups.push(parse_sql_col_exp(group_pair, source_id)?)
                }
            }
            Rule::FromExp => from = parse_exp(some_pair.into_inner().next().unwrap(), source_id)?,
            Rule::FilterExp => {
                let filter_pairs = some_pair.into_inner();
                filters = Vec::with_capacity(filter_pairs.len());
                for filter_pair in filter_pairs {
                    filters.push(parse_exp(filter_pair, source_id)?)
                }
            }
            Rule::SortOp => {
                let sort_pairs = some_pair.into_inner();
                sorts = Vec::with_capacity(sort_pairs.len());
                for sort_pair in sort_pairs {
                    sorts.push(AstNode::Id {
                        name: sort_pair.as_str().to_owned(),
                        start: sort_pair.as_span().start(),
                        source_id,
                    })
                }
            }
            Rule::TakeOp => take = parse_exp(some_pair.into_inner().next().unwrap(), source_id)?,
            unexpected_exp => {
                return Err(raise_error(
                    format!("Unexpected sql: {:?}", unexpected_exp),
                    some_pair.as_span(),
                ))
            }
        }
    }
    Ok(AstNode::Sql {
        op: op.to_owned(),
        ops,
        groups,
        group_type: group_type.to_owned(),
        from: Box::new(from),
        filters,
        sorts,
        take: Box::new(take),
        source_id,
        start: span.start(),
    })
}

fn parse_sql_col_exp(pair: Pair<Rule>, source_id: usize) -> Result<AstNode, PestError<Rule>> {
    match pair.as_rule() {
        Rule::SeriesExp => parse_sql_col_exp(pair.into_inner().next().unwrap(), source_id),
        Rule::RenameSeriesExp => {
            let mut pairs = pair.into_inner();
            let name = pairs.next().unwrap().as_str();
            let exp = parse_exp(pairs.next().unwrap(), source_id)?;
            Ok(AstNode::Series {
                name: name.to_owned(),
                exp: Box::new(exp),
            })
        }
        Rule::SeriesName => Ok(AstNode::Id {
            name: pair.as_str().to_owned(),
            start: pair.as_span().start(),
            source_id,
        }),
        _ => parse_exp(pair, source_id),
    }
}

fn raise_error(msg: String, span: Span) -> PestError<Rule> {
    PestError::new_from_span(ErrorVariant::CustomError { message: msg }, span)
}

pub fn parse(source: &str, source_id: usize) -> Result<Vec<AstNode>, PestError<Rule>> {
    let mut ast = vec![];
    let pairs = JParser::parse(Rule::Program, source).map_err(|e| {
        if e.to_string().len() > 200 {
            match e.location {
                pest::error::InputLocation::Pos(pos) => {
                    let span = Span::new(source, pos, pos + 1)
                        .unwrap_or(Span::new(source, pos, pos).unwrap());
                    match span.as_str() {
                        ":" => raise_error("perhaps '='".to_string(), span),
                        _ => raise_error("syntax error".to_string(), span),
                    }
                }
                pest::error::InputLocation::Span(_) => e,
            }
        } else {
            match e.location {
                pest::error::InputLocation::Pos(pos) => {
                    let span = Span::new(source, pos, pos + 1)
                        .unwrap_or(Span::new(source, pos, pos).unwrap());
                    match span.as_str() {
                        "=" => raise_error("perhaps '=='".to_string(), span),
                        _ => e,
                    }
                }
                pest::error::InputLocation::Span(_) => e,
            }
        }
    })?;
    for pair in pairs {
        if let Rule::Exp = pair.as_rule() {
            ast.push(parse_exp(pair, source_id)?);
        }
    }
    Ok(ast)
}

pub fn parse_date(date: &str) -> Result<i32, String> {
    match chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
        Ok(d) => Ok(d.num_days_from_ce()),
        Err(_) => Err(format!("Not a valid date, {}", date)),
    }
}

pub fn parse_time(time: &str) -> Result<i64, String> {
    let err = || format!("Not a valid time, {}", time);
    let mut nano = "";
    let time = if time.len() > 8 {
        let v: Vec<&str> = time.split(".").collect();
        nano = v[1];
        v[0]
    } else {
        time
    };
    let v: Vec<&str> = time.split(":").collect();
    let hh = v[0].parse::<i64>().map_err(|_| err())?;
    if hh > 23 {
        return Err(err());
    }
    let mm = v[1].parse::<i64>().map_err(|_| err())?;
    if mm > 59 {
        return Err(err());
    }
    let ss = v[2].parse::<i64>().map_err(|_| err())?;
    if ss > 59 {
        return Err(err());
    }
    let nano = format!("{:0<9}", nano);
    let nano = nano.parse::<i64>().map_err(|_| err())?;
    if nano > 999_999_999 {
        return Err(err());
    }
    Ok((hh * 3600 + mm * 60 + ss) * 1000_000_000 + nano)
}

pub fn parse_duration(duration: &str) -> Result<i64, String> {
    let err = || format!("Not a valid duration, {}", duration);
    if duration.contains("D") {
        let v: Vec<&str> = duration.split("D").collect();
        let time = v[1];
        let is_neg = duration.starts_with("-");
        let day = v[0].parse::<i64>().map_err(|_| err())?;
        let nano = if time == "" {
            0
        } else {
            parse_time(time).map_err(|_| err())?
        };
        Ok(if is_neg {
            day * NS_IN_DAY - nano
        } else {
            day * NS_IN_DAY + nano
        })
    } else if duration.ends_with("ns") {
        duration[..duration.len() - 2]
            .parse::<i64>()
            .map_err(|_| err())
    } else if duration.ends_with("s") {
        duration[..duration.len() - 1]
            .parse::<i64>()
            .map_err(|_| err())
            .map(|u| u * 1000_000_000)
    } else if duration.ends_with("m") {
        duration[..duration.len() - 1]
            .parse::<i64>()
            .map_err(|_| err())
            .map(|u| u * 60_000_000_000)
    } else if duration.ends_with("h") {
        duration[..duration.len() - 1]
            .parse::<i64>()
            .map_err(|_| err())
            .map(|u| u * 3_600_000_000_000)
    } else {
        return Err(err());
    }
}

pub fn parse_datetime(dt: &str) -> Result<i64, String> {
    let datetime = if dt.ends_with("T") {
        format!("{}00:00:00.0", dt)
    } else {
        dt.to_owned()
    };
    match chrono::NaiveDateTime::parse_from_str(&datetime, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(d) => Ok(Local.from_local_datetime(&d).unwrap().timestamp_millis()),
        Err(_) => Err(format!("Not a valid datetime, {}", dt)),
    }
}

pub fn parse_timestamp(ts: &str) -> Result<i64, String> {
    let timestamp = if ts.ends_with("D") {
        format!("{}00:00:00.0", ts)
    } else {
        ts.to_owned()
    };
    match chrono::NaiveDateTime::parse_from_str(&timestamp, "%Y-%m-%dD%H:%M:%S%.f") {
        Ok(d) => Ok(Local
            .from_local_datetime(&d)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap_or(0)),
        Err(_) => Err(format!("Not a valid timestamp, {}", ts)),
    }
}

pub fn is_keyword(s: &str) -> bool {
    if vec![
        "select", "update", "delete", "group", "by", "from", "where", "order", "take", "sort",
        "if", "exit", "while", "try", "catch", "return", "raise", "fn", "df", "true", "false",
        "null",
    ]
    .contains(&s)
    {
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::{parse_duration, parse_time};

    #[test]
    fn test_parse_time() {
        assert_eq!(parse_time("23:59:59").unwrap(), 86399000000000);
        assert_eq!(parse_time("07:59:59").unwrap(), 28799000000000);
        assert_eq!(parse_time("23:59:59.").unwrap(), 86399000000000);
        assert_eq!(parse_time("23:59:59.123456789").unwrap(), 86399123456789);
        assert_eq!(parse_time("23:59:59.123").unwrap(), 86399123000000);
        assert_eq!(parse_time("23:59:59.000123").unwrap(), 86399000123000);
        assert!(parse_time("24:59:59.123456789").is_err())
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("-0D23:59:59").unwrap(), -86399000000000);
        assert_eq!(parse_duration("0D23:59:59").unwrap(), 86399000000000);
        assert_eq!(parse_duration("1D23:59:59").unwrap(), 172799000000000);
        assert_eq!(parse_duration("100D23:59:59").unwrap(), 8726399000000000);
        assert!(parse_duration("100D23:60:59.123456789").is_err())
    }
}
