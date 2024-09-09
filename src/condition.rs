use std::collections::HashMap;

#[derive(Debug)]
pub enum Condition {
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    Comparison(String, ComparisonOp, String),
}

#[derive(Debug)]
pub enum ComparisonOp {
    Eq,  // =
    Neq, // !=
    Gt,  // >
    Lt,  // <
    Gte, // >=
    Lte, // <=
}

impl Condition {
    pub fn evaluate(&self, row: &HashMap<String, String>) -> bool {
        match self {
            Condition::And(left, right) => left.evaluate(row) && right.evaluate(row),
            Condition::Or(left, right) => left.evaluate(row) || right.evaluate(row),
            Condition::Not(cond) => !cond.evaluate(row),
            Condition::Comparison(col, op, val) => {
                if let Some(data_val) = row.get(col) {
                    match op {
                        ComparisonOp::Eq => data_val == val,
                        ComparisonOp::Neq => data_val != val,
                        ComparisonOp::Gt => data_val > val,
                        ComparisonOp::Lt => data_val < val,
                        ComparisonOp::Gte => data_val >= val,
                        ComparisonOp::Lte => data_val <= val,
                    }
                } else {
                    false
                }
            }
        }
    }
}
