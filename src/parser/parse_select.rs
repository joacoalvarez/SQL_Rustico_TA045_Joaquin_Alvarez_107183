pub use super::extract_query::{
    expected_next_word, extract_and_parse_order_clause, extract_and_parse_where_clause,
    extract_next_word, extract_table_names,
};
use crate::command_types::Commands;
use crate::errors::ErrorType;

/// Extrae los headers del comando select y los retorna en un Vector de string
pub fn extract_select_headers(s: &mut String) -> Result<Vec<String>, ErrorType> {
    let mut result = Vec::new();
    let trimmed = s.trim_start();

    // obtengo lo que haya entre SELECT y FROM
    if let Some(from) = trimmed.find("FROM") {
        let aux = trimmed[..from].trim();
        result.extend(aux.split(',').map(|x| x.trim().to_string()));
        if result.is_empty() || result.first().map_or(true, String::is_empty) {
            return Err(ErrorType::InvalidSyntax(
                "Expected headers or '*' after 'SELECT' command".into(),
            ));
        }
        *s = trimmed[from..].to_string();
        Ok(result)
    } else {
        Err(ErrorType::InvalidSyntax(
            "Expected 'FROM' after headers".into(),
        ))
    }
}

pub fn select_parser(query: &mut String) -> Result<Commands, ErrorType> {
    let headers = extract_select_headers(query)?;
    expected_next_word(query, "FROM")?;
    let tables = extract_table_names(query)?;

    let mut where_st = None;
    let mut order = None;

    while let Some(word) = extract_next_word(query) {
        match word.as_str() {
            "WHERE" => where_st = Some(extract_and_parse_where_clause(query)?),
            "ORDER" => order = Some(extract_and_parse_order_clause(query)?),
            ";" => break,
            _ => {
                return Err(ErrorType::InvalidSyntax(format!(
                    "Expected 'WHERE', 'ORDER BY', or end of query. Found {word}"
                )))
            }
        }
    }

    Ok(Commands::Select {
        headers,
        tables,
        where_st,
        order,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_parser_with_where_and_order() {
        let mut query = String::from("name, age FROM users WHERE age > 18 ORDER BY age DESC;");
        let result = select_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Select {
            headers,
            tables,
            where_st,
            order,
        } = result.unwrap()
        {
            assert_eq!(headers, vec!["name", "age"]);
            assert_eq!(tables, ["users"]);
            assert!(where_st.is_some());
            assert!(order.is_some());
        } else {
            panic!("Expected a Select command");
        }
    }

    #[test]
    fn test_select_parser_without_where_and_order() {
        let mut query = String::from("name, age FROM users;");
        let result = select_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Select {
            headers,
            tables,
            where_st,
            order,
        } = result.unwrap()
        {
            assert_eq!(headers, vec!["name", "age"]);
            assert_eq!(tables, ["users"]);
            assert!(where_st.is_none());
            assert!(order.is_none());
        } else {
            panic!("Expected a Select command");
        }
    }

    #[test]
    fn test_select_parser_missing_from() {
        let mut query = String::from("name, age;");
        let result = select_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_parser_missing_from_keyword() {
        let mut query = String::from("name, age WHERE age > 18;");
        let result = select_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_parser_incorrect_order_by_syntax() {
        let mut query = String::from("name, age FROM users ORDER BY;");
        let result = select_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_parser_incorrect_order_syntax() {
        let mut query = String::from("name, age FROM users ORDER;");
        let result = select_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_parser_missing_values() {
        let mut query = String::from("FROM users;");
        let result = select_parser(&mut query);
        assert!(result.is_err());
    }
}
