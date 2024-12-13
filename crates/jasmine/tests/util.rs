use jasmine::Rule;
use pest::iterators::{Pair, Pairs};
fn pretty_format_rule(pair: Pair<Rule>, indent: usize) -> String {
    let mut s = format!("{:?}", pair.as_rule());
    let mut inners = pair.into_inner();
    if inners.len() == 0 {
        s + "\n"
    } else if inners.len() == 1 {
        s.push_str(" -> ");
        s.push_str(&pretty_format_rule(inners.next().unwrap(), indent + 1));
        s
    } else {
        s.push_str("\n");
        while let Some(p) = inners.next() {
            for _ in 0..indent {
                s.push_str("  ")
            }
            s.push_str(" -> ");
            s.push_str(&pretty_format_rule(p, indent + 1))
        }
        s
    }
}

pub fn pretty_format_rules(mut pairs: Pairs<Rule>) -> String {
    let mut s = "".to_owned();
    while let Some(p) = pairs.next() {
        s.push_str(&pretty_format_rule(p, 0))
    }
    s
}
