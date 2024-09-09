use crate::condition::{ComparisonOp, Condition};
use crate::order::{OrderBy, OrderDirection};
use std::collections::HashMap;
use std::error::Error;

pub fn extract_next_word(s: &mut String) -> Option<String> {
    let trimmed = s.trim_start();

    // le saco los ' ' a los strings
    if let Some(stripped) = trimmed.strip_prefix('\'') {
        if let Some(end_quote_pos) = stripped.find('\'') {
            let word = trimmed[0..=end_quote_pos + 1].to_string();
            *s = trimmed[end_quote_pos + 2..].trim_start().to_string();
            return Some(word);
        } else {
            return None;
        }
    }

    let symbols = [' ', ',', '(', ')', ';'];

    if let Some(i) = trimmed.chars().position(|x| symbols.contains(&x)) {
        let word = trimmed[..i].to_string();
        *s = trimmed[i + 1..].to_string();
        Some(word)
    } else if let Some(i) = trimmed.find(',') {
        let word = trimmed[..i].to_string();
        *s = trimmed[i + 1..].to_string();
        Some(word)
    } else if !trimmed.is_empty() {
        let word = trimmed.to_string();
        s.clear();
        Some(word)
    } else {
        None
    }
}

pub fn get_next_char(s: &mut str) -> Option<char> {
    let trimmed = s.trim_start();
    trimmed.chars().next()
}

pub fn extract_between_parenthesis(s: &mut String) -> Result<Vec<String>, Box<dyn Error>> {
    let mut result = Vec::new();
    let trimmed = s.trim_start();

    if !trimmed.starts_with('(') {
        return Err("No opening parenthesis found".into());
    }

    if let Some(closing_parenthesis) = trimmed.find(')') {
        let aux = trimmed[1..closing_parenthesis].trim();
        result.extend(aux.split(',').map(|x| {
            let value = x.trim();
            // en caso de que el value este entre ' '
            if value.starts_with('\'') && value.ends_with('\'') && value.len() > 1 {
                value[1..value.len() - 1].to_string()
            } else {
                value.to_string()
            }
        }));
        *s = trimmed[closing_parenthesis + 1..].to_string();
        Ok(result)
    } else {
        Err("No closing parenthesis found".into())
    }
}

pub fn extract_select_headers(s: &mut String) -> Result<Vec<String>, Box<dyn Error>> {
    let mut result = Vec::new();
    let trimmed = s.trim_start();

    if let Some(from) = trimmed.find("FROM") {
        let aux = trimmed[..from].trim();
        result.extend(aux.split(',').map(|x| x.trim().to_string()));
        if result.is_empty() || result.first().map_or(true, |x| x.is_empty()) {
            return Err("Expected headers or '*' after 'SELECT' command".into());
        }
        *s = trimmed[from..].to_string();
        Ok(result)
    } else {
        Err("Expected 'FROM' after headers".into())
    }
}

pub fn extract_updates(s: &mut String) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let mut result = HashMap::new();
    let trimmed = s.trim_start();

    // si no encuentra where o ';', llegó al final del s update
    let updates_end = trimmed
        .find("WHERE") // o terminan con el where
        .or_else(|| trimmed.find(";")) // o con el end of s
        .unwrap_or(trimmed.len()); // no deberia llegar aca

    for update in trimmed[..updates_end].trim().split(',') {
        let update = update.trim();
        if let Some(equal) = update.find('=') {
            let key = update[..equal].trim().to_string();
            let value = update[equal + 1..].trim().to_string();

            //por si el value esta entre ' '
            let value = value.trim_matches('\'').to_string();

            if key.is_empty() {
                return Err("Key in update expression cannot be empty".into());
            }

            result.insert(key, value);
        } else {
            return Err(format!("No equal sign found in update: {}", update).into());
        }
    }

    *s = trimmed[updates_end..].to_string();
    Ok(result)
}

pub fn extract_and_parse_where_clause(s: &mut String) -> Result<Condition, Box<dyn Error>> {
    parse_or_condition(s)
}

fn parse_or_condition(s: &mut String) -> Result<Condition, Box<dyn Error>> {
    let mut left = parse_and_condition(s)?;
    while let Some(word) = extract_next_word(s) {
        // frenar si hay ORDER o ;
        if word == "ORDER" || word == ";" {
            s.insert_str(0, &format!("{} ", word));
            break;
        }

        if word == "OR" {
            let right = parse_and_condition(s)?;
            left = Condition::Or(Box::new(left), Box::new(right));
        } else {
            // Si no es un OR, devolvemos el word al inicio del query restante
            s.insert_str(0, &format!("{} ", word));
            break;
        }
    }
    Ok(left)
}

fn parse_and_condition(s: &mut String) -> Result<Condition, Box<dyn Error>> {
    let mut left = parse_not_condition(s)?;
    while let Some(word) = extract_next_word(s) {
        // frenar si hay order o ;
        if word == "ORDER" || word == ";" {
            s.insert_str(0, &format!("{} ", word));
            break;
        }
        if word == "AND" {
            let right = parse_not_condition(s)?;
            left = Condition::And(Box::new(left), Box::new(right));
        } else {
            // Si no es un AND, devolvemos el word al inicio del query restante
            s.insert_str(0, &format!("{} ", word));
            break;
        }
    }
    Ok(left)
}

fn parse_not_condition(s: &mut String) -> Result<Condition, Box<dyn Error>> {
    if let Some(word) = extract_next_word(s) {
        // frenar si hay order o ;
        if word == "ORDER" || word == ";" {
            s.insert_str(0, &format!("{} ", word));
            return Err("Unexpected end of condition".into());
        }
        if word == "NOT" {
            let condition = parse_comparison_condition(s)?;
            return Ok(Condition::Not(Box::new(condition)));
        } else {
            // si no hay esta condicion vuelvo a insertar el texto (no muy optimo pero tarde)
            s.insert_str(0, &format!("{} ", word));
        }
    }
    parse_comparison_condition(s)
}

fn parse_comparison_condition(s: &mut String) -> Result<Condition, Box<dyn Error>> {
    let trimmed = s.trim_start();
    if trimmed.starts_with('(') {
        *s = s[1..].to_string();
        // parsea la subcondición dentro del paréntesis
        let condition = extract_and_parse_where_clause(s)?;

        let closing_trimmed = s.trim_start();
        if let Some(stripped) = closing_trimmed.strip_prefix(')') {
            *s = stripped.trim_start().to_string();
        } else {
            return Err("Missing closing ')'".into());
        }
        return Ok(condition);
    }

    let left = extract_next_word(s).ok_or("Missing left operand")?;
    let op = extract_next_word(s).ok_or("Missing operator")?;
    let mut right = extract_next_word(s).ok_or("Missing right operand")?;

    // por si el valor a comparar está con ' '
    if right.starts_with('\'') {
        let mut combined = right;
        while !combined.ends_with('\'') {
            let next = extract_next_word(s).ok_or("Missing closing quote on right operand")?;
            combined.push(' ');
            combined.push_str(&next);
        }
        right = combined[1..combined.len() - 1].to_string(); // quita las comillas
    }

    let comparison_op = match op.as_str() {
        "=" => ComparisonOp::Eq,
        "!=" => ComparisonOp::Neq,
        ">" => ComparisonOp::Gt,
        "<" => ComparisonOp::Lt,
        ">=" => ComparisonOp::Gte,
        "<=" => ComparisonOp::Lte,
        _ => return Err(format!("Unsupported comparison operator: {}", op).into()),
    };

    Ok(Condition::Comparison(left, comparison_op, right))
}

pub fn extract_and_parse_order_clause(s: &mut String) -> Result<Vec<OrderBy>, Box<dyn Error>> {
    let mut order_by = Vec::new();

    // busco el by despues del order
    if extract_next_word(s).ok_or("Expected 'BY' keyword")? != "BY" {
        return Err("Expected 'BY' keyword after 'ORDER'".into());
    }

    let mut first_word = false;
    while let Some(column) = extract_next_word(s) {
        if column == ";" {
            s.insert(0, ';');
            break;
        }

        if !first_word {
            if column == ";" {
                return Err("The first word cannot be ';'".into());
            }
            first_word = true;
        }

        let direction = if let Some(next_word) = extract_next_word(s) {
            match next_word.as_str() {
                "ASC" => OrderDirection::Ascending,
                "DESC" => OrderDirection::Descending,
                ";" => {
                    s.insert(0, ';');
                    OrderDirection::Ascending // como default ordena ascendentemente
                }
                _ => {
                    s.insert_str(0, &format!("{} ", next_word));
                    return Err(format!("Unexpected token after column: {}", next_word).into());
                }
            }
        } else {
            OrderDirection::Ascending // como default ordena ascendentemente
        };

        order_by.push(OrderBy { column, direction });
    }

    if !first_word {
        return Err("Expected at least one column to sort by".into());
    }

    Ok(order_by)
}
