use super::extract_query::{
    expected_next_word, extract_and_parse_where_clause, extract_next_word, extract_table_names,
    strip_single_quotes
};
use crate::command_types::Commands;
use crate::condition::Condition;
use crate::errors::ErrorType;
use std::collections::HashMap;

/// Extrae los campos y valores a actualizar y los retorna en un hashmap
fn extract_updates(s: &mut String) -> Result<HashMap<String, String>, ErrorType> {
    let mut result = HashMap::new();
    let trimmed = s.trim_start();

    // si no encuentra where o ';', llegó al final del s update
    let updates_end = trimmed
        .find("WHERE") // o terminan con el where
        .or_else(|| trimmed.find(';')) // o con el end of s
        .unwrap_or(trimmed.len()); // no deberia llegar aca

    for update in trimmed[..updates_end].trim().split(',') {
        let update = update.trim();
        if let Some(equal) = update.find('=') {
            let key = update[..equal].trim().to_string();
            let value = update[equal + 1..].trim().to_string();

            //por si el value esta entre ' '
            let value = strip_single_quotes(&value);

            if key.is_empty() {
                return Err(ErrorType::InvalidSyntax(
                    "Key in update expression cannot be empty".into(),
                ));
            }

            result.insert(key, value);
        } else {
            return Err(ErrorType::InvalidSyntax(format!(
                "No equal sign found in update: {update}"
            )));
        }
    }

    *s = trimmed[updates_end..].to_string();
    Ok(result)
}

pub fn update_parser(query: &mut String) -> Result<Commands, ErrorType> {
    let tables = extract_table_names(query)?;
    expected_next_word(query, "SET")?;

    //busco los sets
    let updates = extract_updates(query)?;

    let where_st: Option<Condition> = match extract_next_word(query) {
        // busco si hay where o se termino la query
        Some(word) if word == "WHERE" => {
            //implementacion where
            Some(extract_and_parse_where_clause(query)?)
        }
        Some(word) if word == ";" => None,
        None => None,
        Some(_) => {
            return Err(ErrorType::InvalidSyntax(
                "Expected WHERE or end of query, found something else".into(),
            ))
        }
    };

    Ok(Commands::Update {
        tables,
        updates,
        where_st,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_parser_with_where() {
        let mut query = String::from("users SET name = 'Juan' WHERE id = 1;");
        let result = update_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Update {
            tables,
            updates,
            where_st,
        } = result.unwrap()
        {
            assert_eq!(tables, ["users"]);
            assert_eq!(updates.get("name"), Some(&"Juan".to_string()));
            assert!(where_st.is_some());
        } else {
            panic!("Expected an Update command");
        }
    }

    #[test]
    fn test_update_parser_without_where() {
        let mut query = String::from("users SET name = 'Juan';");
        let result = update_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Update {
            tables,
            updates,
            where_st,
        } = result.unwrap()
        {
            assert_eq!(tables, ["users"]);
            assert_eq!(updates.get("name"), Some(&"Juan".to_string()));
            assert!(where_st.is_none());
        } else {
            panic!("Expected an Update command");
        }
    }

    #[test]
    fn test_update_parser_missing_set() {
        let mut query = String::from("users WHERE id = 1;");
        let result = update_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_parser_missing_set_keyword() {
        let mut query = String::from("users name = 'Juan';");
        let result = update_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_parser_incorrect_update_syntax() {
        let mut query = String::from("users SET name 'Juan';");
        let result = update_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_parser_empty_key() {
        let mut query = String::from("users SET = 3 WHERE id = 1;");
        let result = update_parser(&mut query);
        assert!(result.is_err());
    }

    // permito valores vacios
    #[test]
    fn test_update_parser_empty_value() {
        let mut query = String::from("users SET name = WHERE id = 1;");
        let result = update_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Update {
            tables,
            updates,
            where_st,
        } = result.unwrap()
        {
            assert_eq!(tables, ["users"]);
            assert_eq!(updates.get("name"), Some(&"".to_string()));
            assert!(where_st.is_some());
        } else {
            panic!("Expected an Update command");
        }
    }
}
