use std::collections::HashMap;

#[derive(Debug)]
/// Representacion de las distintas condiciones logicas
pub enum Condition {
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    Comparison(String, ComparisonOp, String),
}

#[derive(Debug)]
/// Representacion de los distintos operandos de comparacion
pub enum ComparisonOp {
    Eq,  // =
    Neq, // !=
    Gt,  // >
    Lt,  // <
    Gte, // >=
    Lte, // <=
}

impl Condition {
    /// Evalua las condiciones del where en forma de arbol, si la columna no existe retorna false
    #[must_use]
    pub fn evaluate(&self, row: &HashMap<String, String>) -> bool {
        match self {
            Condition::And(left, right) => left.evaluate(row) && right.evaluate(row),
            Condition::Or(left, right) => left.evaluate(row) || right.evaluate(row),
            Condition::Not(cond) => !cond.evaluate(row),
            Condition::Comparison(col, op, val) => {
                if let Some(data_val) = row.get(col) {
                    // Chequea sin son ambos numeros y los parsea
                    if let (Ok(data_int), Ok(val_int)) =
                        (data_val.parse::<i32>(), val.parse::<i32>())
                    {
                        match op {
                            ComparisonOp::Eq => data_int == val_int,
                            ComparisonOp::Neq => data_int != val_int,
                            ComparisonOp::Gt => data_int > val_int,
                            ComparisonOp::Lt => data_int < val_int,
                            ComparisonOp::Gte => data_int >= val_int,
                            ComparisonOp::Lte => data_int <= val_int,
                        }
                    } else {
                        match op {
                            ComparisonOp::Eq => data_val == val,
                            ComparisonOp::Neq => data_val != val,
                            ComparisonOp::Gt => data_val > val,
                            ComparisonOp::Lt => data_val < val,
                            ComparisonOp::Gte => data_val >= val,
                            ComparisonOp::Lte => data_val <= val,
                        }
                    }
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_comparison() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());

        // "amount" == "100"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Eq, "100".to_string());
        assert!(condition.evaluate(&row)); // true, 100 == 100

        // "amount" == "101"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Eq, "101".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 != 1000

        // "amount" == "99"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Eq, "101".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 != 1000

        // no existe la columna
        let condition =
            Condition::Comparison("noexiste".to_string(), ComparisonOp::Eq, "100".to_string());
        assert!(!condition.evaluate(&row)); // false
    }

    #[test]
    fn test_neq_comparison() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());

        // "amount" != "150"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Neq, "150".to_string());
        assert!(condition.evaluate(&row)); // true, 100 != 150

        // "amount" != "100"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Neq, "100".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 == 100

        // no existe la columna
        let condition =
            Condition::Comparison("noexiste".to_string(), ComparisonOp::Neq, "100".to_string());
        assert!(!condition.evaluate(&row));
    }

    #[test]
    fn test_gt_comparison() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());

        // "amount" > "50"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Gt, "50".to_string());
        assert!(condition.evaluate(&row)); // true, 100 > 50

        // "amount" > "100"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Gt, "100".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 == 100

        // "amount" > "150"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Gt, "150".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 < 150
    }

    #[test]
    fn test_lt_comparison() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());

        // "amount" < "150"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Lt, "150".to_string());
        assert!(condition.evaluate(&row)); // true, 100 < 150

        // "amount" < "100"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Lt, "100".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 == 100

        // "amount" < "50"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Lt, "50".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 > 50
    }

    #[test]
    fn test_gte_comparison() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());

        // "amount" >= "100"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Gte, "100".to_string());
        assert!(condition.evaluate(&row)); // true, 100 >= 100

        // "amount" >= "50"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Gte, "50".to_string());
        assert!(condition.evaluate(&row)); // true, 100 >= 50

        // "amount" >= "150"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Gte, "150".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 < 150
    }

    #[test]
    fn test_lte_comparison() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());

        // "amount" <= "100"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Lte, "100".to_string());
        assert!(condition.evaluate(&row)); // true, 100 <= 100

        // "amount" <= "150"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Lte, "150".to_string());
        assert!(condition.evaluate(&row)); // true, 100 <= 150

        // "amount" <= "50"
        let condition =
            Condition::Comparison("amount".to_string(), ComparisonOp::Lte, "50".to_string());
        assert!(!condition.evaluate(&row)); // false, 100 > 50
    }

    #[test]
    fn test_nested_and_conditions() {
        let mut row = HashMap::new();
        row.insert("name".to_string(), "marco".to_string());
        row.insert("price".to_string(), "50".to_string());
        row.insert("quantity".to_string(), "5".to_string());

        // (amount == 100) AND (price != 50) AND (quantity < 10)
        let condition = Condition::And(
            Box::new(Condition::Comparison(
                "name".to_string(),
                ComparisonOp::Eq,
                "marco".to_string(),
            )),
            Box::new(Condition::And(
                Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Neq,
                    "50".to_string(),
                )),
                Box::new(Condition::Comparison(
                    "quantity".to_string(),
                    ComparisonOp::Lt,
                    "10".to_string(),
                )),
            )),
        );
        assert!(!condition.evaluate(&row)); // false, price == 50
    }

    #[test]
    fn test_nested_or_conditions() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());
        row.insert("price".to_string(), "50".to_string());
        row.insert("quantity".to_string(), "5".to_string());

        // (amount > 150) OR (price < 40) OR (quantity <= 5)
        let condition = Condition::Or(
            Box::new(Condition::Comparison(
                "amount".to_string(),
                ComparisonOp::Gt,
                "150".to_string(),
            )),
            Box::new(Condition::Or(
                Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Lt,
                    "40".to_string(),
                )),
                Box::new(Condition::Comparison(
                    "quantity".to_string(),
                    ComparisonOp::Lte,
                    "5".to_string(),
                )),
            )),
        );
        assert!(condition.evaluate(&row)); // true, quantity <= 5
    }

    #[test]
    fn test_complex_nested_conditions() {
        let mut row = HashMap::new();
        row.insert("amount".to_string(), "100".to_string());
        row.insert("price".to_string(), "50".to_string());
        row.insert("quantity".to_string(), "5".to_string());

        // Condición compleja: (amount == 100) AND (price == 50) OR NOT (quantity < 3)
        let condition = Condition::Or(
            Box::new(Condition::And(
                Box::new(Condition::Comparison(
                    "amount".to_string(),
                    ComparisonOp::Eq,
                    "100".to_string(),
                )),
                Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Eq,
                    "50".to_string(),
                )),
            )),
            Box::new(Condition::Not(Box::new(Condition::Comparison(
                "quantity".to_string(),
                ComparisonOp::Lt,
                "3".to_string(),
            )))),
        );
        assert!(condition.evaluate(&row)); // true, amount == 100 y quantity >= 3

        // NOT ((amount < 50) OR (price > 60)) AND (quantity <= 5)
        let condition = Condition::And(
            Box::new(Condition::Not(Box::new(Condition::Or(
                Box::new(Condition::Comparison(
                    "amount".to_string(),
                    ComparisonOp::Lt,
                    "50".to_string(),
                )),
                Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Gt,
                    "60".to_string(),
                )),
            )))),
            Box::new(Condition::Comparison(
                "quantity".to_string(),
                ComparisonOp::Lte,
                "5".to_string(),
            )),
        );
        assert!(condition.evaluate(&row)); // true, amount > 100 y quantity == 5

        // (amount > 50) AND (price < 100) OR (quantity != 5)
        let condition = Condition::Or(
            Box::new(Condition::And(
                Box::new(Condition::Comparison(
                    "amount".to_string(),
                    ComparisonOp::Gt,
                    "50".to_string(),
                )),
                Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Lt,
                    "100".to_string(),
                )),
            )),
            Box::new(Condition::Comparison(
                "quantity".to_string(),
                ComparisonOp::Neq,
                "5".to_string(),
            )),
        );
        assert!(condition.evaluate(&row)); // true, amount > 50 y price < 100

        // (amount == 100) OR NOT (price <= 50) AND (quantity < 10)
        let condition = Condition::And(
            Box::new(Condition::Or(
                Box::new(Condition::Comparison(
                    "amount".to_string(),
                    ComparisonOp::Eq,
                    "100".to_string(),
                )),
                Box::new(Condition::Not(Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Lte,
                    "50".to_string(),
                )))),
            )),
            Box::new(Condition::Comparison(
                "quantity".to_string(),
                ComparisonOp::Lt,
                "10".to_string(),
            )),
        );
        assert!(condition.evaluate(&row)); // true, amount == 100

        // NOT ((amount == 100) AND (price == 50)) OR (quantity > 5)
        let condition = Condition::Or(
            Box::new(Condition::Not(Box::new(Condition::And(
                Box::new(Condition::Comparison(
                    "amount".to_string(),
                    ComparisonOp::Eq,
                    "100".to_string(),
                )),
                Box::new(Condition::Comparison(
                    "price".to_string(),
                    ComparisonOp::Eq,
                    "50".to_string(),
                )),
            )))),
            Box::new(Condition::Comparison(
                "quantity".to_string(),
                ComparisonOp::Gte,
                "5".to_string(),
            )),
        );
        assert!(condition.evaluate(&row)); // true, quantity <= 5
    }
}
