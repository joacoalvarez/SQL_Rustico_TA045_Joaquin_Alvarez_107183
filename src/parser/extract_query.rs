use crate::condition::{ComparisonOp, Condition};
use crate::errors::ErrorType;
use crate::order::{Direction, OrderBy};

/// Corrobora que la siguiente palabra sea la esperada
pub fn expected_next_word(s: &mut String, expected: &str) -> Result<(), ErrorType> {
    match extract_next_word(s) {
        Some(word) if word == expected => Ok(()),
        Some(word) => Err(ErrorType::InvalidSyntax(format!(
            "Expected '{expected}', found '{word}'"
        ))),
        None => Err(ErrorType::InvalidSyntax(format!(
            "Expected '{expected}', found nothing"
        ))),
    }
}

/// Extrae los nombres de la tabla de la query
pub fn extract_table_names(s: &mut String) -> Result<Vec<String>, ErrorType> {
    let mut tables = Vec::new();

    while let Some(table) = extract_next_word(s) {
        tables.push(table);

        // Si encuentra un ',' sigue extrayendo tablas, sino termina
        if let Some(next_char) = get_next_char(s) {
            if next_char != ',' {
                break;
            }
        }
    }

    if tables.is_empty() {
        Err(ErrorType::InvalidSyntax("Missing table names".to_string()))
    } else {
        Ok(tables)
    }
}

/// Verifica si una cadena esta entre comillas simples y, de ser asi, las quita.
pub fn strip_single_quotes(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() > 1 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

/// Extrae la siguiente palabra y mueve el inicio del string antes de la siguiente.
pub fn extract_next_word(s: &mut String) -> Option<String> {
    let trimmed = s.trim_start();

    let symbols = [' ', ',', '(', ')', ';'];

    if trimmed == ";" {
        Some(trimmed.to_string())
    } else if let Some(i) = trimmed.chars().position(|x| symbols.contains(&x)) {
        let word = trimmed[..i].to_string();
        *s = trimmed[i + 1..].to_string();
        Some(strip_single_quotes(&word))
    } else if !trimmed.is_empty() {
        let word = trimmed.to_string();
        s.clear();
        Some(strip_single_quotes(&word))
    } else {
        None
    }
}

/// Obtiene el siguiente caracter
pub fn get_next_char(s: &mut str) -> Option<char> {
    let trimmed = s.trim_start();
    trimmed.chars().next()
}

/// Extrae los valores dentro de un parentesis y los devuelve en un Vector de String
pub fn extract_between_parenthesis(s: &mut String) -> Result<Vec<String>, ErrorType> {
    let mut result = Vec::new();
    let trimmed = s.trim_start();

    if !trimmed.starts_with('(') {
        return Err(ErrorType::InvalidSyntax(
            "No opening parenthesis found".into(),
        ));
    }

    if let Some(closing_parenthesis) = trimmed.find(')') {
        let aux = trimmed[1..closing_parenthesis].trim(); // toma el string detro de '(' ')'
        result.extend(aux.split(',').map(|x| {
            let value = x.trim();
            strip_single_quotes(value) // Retorna el valor sin comillas
        }));
        *s = trimmed[closing_parenthesis + 1..].to_string();
        Ok(result)
    } else {
        Err(ErrorType::InvalidSyntax(
            "No closing parenthesis found".into(),
        ))
    }
}

/// Extrae y parsea la condicion where
pub fn extract_and_parse_where_clause(s: &mut String) -> Result<Condition, ErrorType> {
    // Busca ORDER, si no esta se posiciona antes del ';' final
    let clause_end_pos = s.find("ORDER").unwrap_or(s.len() - 1);

    let mut extracted_str = s[..clause_end_pos].trim().to_string();

    *s = s[clause_end_pos..].to_string();

    parse_or_condition(&mut extracted_str)
}

/// Busca condicion or
fn parse_or_condition(s: &mut String) -> Result<Condition, ErrorType> {
    let mut left = parse_and_condition(s)?;
    while let Some(word) = extract_next_word(s) {
        if word == "OR" {
            let right = parse_and_condition(s)?;
            left = Condition::Or(Box::new(left), Box::new(right));
        } else {
            // Si no es un OR, devolvemos el word al inicio del query restante
            s.insert_str(0, &format!("{word} "));
            break;
        }
    }
    Ok(left)
}

/// Busca condicion and
fn parse_and_condition(s: &mut String) -> Result<Condition, ErrorType> {
    let mut left = parse_not_condition(s)?;
    while let Some(word) = extract_next_word(s) {
        if word == "AND" {
            let right = parse_not_condition(s)?;
            left = Condition::And(Box::new(left), Box::new(right));
        } else {
            // Si no es un AND, devolvemos el word al inicio del query restante
            s.insert_str(0, &format!("{word} "));
            break;
        }
    }
    Ok(left)
}

/// Busca condicion not
fn parse_not_condition(s: &mut String) -> Result<Condition, ErrorType> {
    if let Some(word) = extract_next_word(s) {
        if word == "NOT" {
            let condition = parse_comparison_condition(s)?;
            return Ok(Condition::Not(Box::new(condition)));
        }
        // si no hay esta condicion vuelvo a insertar el texto
        s.insert_str(0, &format!("{word} "));
    } else {
        // Si no hay más palabras (None), devolvemos un error
        return Err(ErrorType::InvalidSyntax(
            "Unexpected end of condition after NOT".into(),
        ));
    }
    parse_comparison_condition(s)
}

/// Parsea la comparacion
/// left: columna
/// op: operacion
/// right: valor
fn parse_comparison_condition(s: &mut String) -> Result<Condition, ErrorType> {
    let trimmed = s.trim_start();
    if trimmed.starts_with('(') {
        *s = s[1..].to_string();
        // parsea la subcondición dentro del paréntesis
        let condition = extract_and_parse_where_clause(s)?;

        let closing_trimmed = s.trim_start();
        if let Some(stripped) = closing_trimmed.strip_prefix(')') {
            *s = stripped.trim_start().to_string();
        } else {
            return Err(ErrorType::InvalidSyntax("Missing closing ')'".into()));
        }
        return Ok(condition);
    }

    let left =
        extract_next_word(s).ok_or(ErrorType::InvalidSyntax("Missing left operand".to_string()))?;
    let op =
        extract_next_word(s).ok_or(ErrorType::InvalidSyntax("Missing operator".to_string()))?;
    let right = extract_next_word(s).ok_or(ErrorType::InvalidSyntax(
        "Missing right operand".to_string(),
    ))?;
    let right = strip_single_quotes(&right);

    let comparison_op = match op.as_str() {
        "=" => ComparisonOp::Eq,
        "!=" => ComparisonOp::Neq,
        ">" => ComparisonOp::Gt,
        "<" => ComparisonOp::Lt,
        ">=" => ComparisonOp::Gte,
        "<=" => ComparisonOp::Lte,
        _ => {
            return Err(ErrorType::InvalidSyntax(format!(
                "{op} is not a comparison operator"
            )))
        }
    };

    Ok(Condition::Comparison(left, comparison_op, right))
}

/// Extrae y parsea la condicion order
pub fn extract_and_parse_order_clause(s: &mut String) -> Result<Vec<OrderBy>, ErrorType> {
    let mut order_by = Vec::new();

    // busco el by despues del order
    expected_next_word(s, "BY")?;

    // para chequear que haya al menos una condicion de orden
    let mut first_word = false;
    while let Some(column) = extract_next_word(s) {
        if column == ";" && !first_word {
            return Err(ErrorType::InvalidSyntax(
                "The first word cant be ';'".into(),
            ));
        }

        if column == ";" {
            s.insert(0, ';');
        } else {
            first_word = true;
        }

        let direction = if let Some(next_word) = extract_next_word(s) {
            match next_word.as_str() {
                "ASC" => Direction::Ascending,
                "DESC" => Direction::Descending,
                ";" => {
                    s.insert(0, ';');
                    Direction::Ascending // como default ordena ascendentemente
                }
                _ => {
                    s.insert_str(0, &format!("{next_word} "));
                    return Err(ErrorType::InvalidSyntax(format!(
                        "Unexpected word after column: {next_word}"
                    )));
                }
            }
        } else {
            Direction::Ascending // como default ordena ascendentemente
        };

        order_by.push(OrderBy { column, direction });
    }

    if !first_word {
        return Err(ErrorType::InvalidSyntax(
            "Expected at least one column to sort by".into(),
        ));
    }

    Ok(order_by)
}
