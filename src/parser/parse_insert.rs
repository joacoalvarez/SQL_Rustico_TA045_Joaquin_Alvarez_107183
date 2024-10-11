use super::extract_query::{
    expected_next_word, extract_between_parenthesis, extract_table_names, get_next_char,
};
use crate::command_types::Commands;
use crate::errors::ErrorType;

pub fn insert_parser(query: &mut String) -> Result<Commands, ErrorType> {
    expected_next_word(query, "INTO")?;

    let tables = extract_table_names(query)?;

    //busco si hay headers
    let headers = if let Some('(') = get_next_char(query) {
        extract_between_parenthesis(query)?
    } else {
        Vec::new()
    };

    expected_next_word(query, "VALUES")?;

    let values = extract_between_parenthesis(query)?;

    // Valida que el número de headers y values coincide si hay headers
    if !headers.is_empty() && headers.len() != values.len() {
        return Err(ErrorType::InvalidColumn(
            "Number of headers and values must match".into(),
        ));
    }

    Ok(Commands::Insert {
        tables,
        headers,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_parser_with_headers() {
        let mut query = String::from("INTO users (id, name) VALUES (1, 'Juan');");
        let result = insert_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Insert {
            tables,
            headers,
            values,
        } = result.unwrap()
        {
            assert_eq!(tables, ["users"]);
            assert_eq!(headers, vec!["id", "name"]);
            assert_eq!(values, vec!["1", "Juan"]);
        } else {
            panic!("Expected an Insert command");
        }
    }

    #[test]
    fn test_insert_parser_without_headers() {
        let mut query = String::from("INTO users VALUES (1, 'Juan');");
        let result = insert_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Insert {
            tables,
            headers,
            values,
        } = result.unwrap()
        {
            assert_eq!(tables, ["users"]);
            assert!(headers.is_empty());
            assert_eq!(values, vec!["1", "Juan"]);
        } else {
            panic!("Expected an Insert command");
        }
    }

    #[test]
    fn test_insert_parser_missing_values() {
        let mut query = String::from("INTO users (id, name);");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_parser_missing_tables() {
        let mut query = String::from("INTO (id, name) VALUES (1, 'Juan');");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_parser_missing_values_keyword() {
        let mut query = String::from("INTO users (id, name) (1, 'Juan');");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_parser_missing_parentheses() {
        let mut query = String::from("INTO users (id, name) VALUES 1, 'Juan';");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }
}
