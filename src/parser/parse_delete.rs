pub use super::extract_query::{
    expected_next_word, extract_and_parse_where_clause, extract_next_word, extract_table_names,
};
use crate::command_types::Commands;
use crate::condition::Condition;
use crate::errors::ErrorType;

pub fn delete_parser(query: &mut String) -> Result<Commands, ErrorType> {
    expected_next_word(query, "FROM")?;

    let tables = extract_table_names(query)?;

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

    Ok(Commands::Delete { tables, where_st })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_parser_with_where() {
        let mut query = String::from("FROM users WHERE id = 1;");
        let result = delete_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Delete { tables, where_st } = result.unwrap() {
            assert_eq!(tables, ["users"]);
            assert!(where_st.is_some());
        } else {
            panic!("Expected a Delete command");
        }
    }

    #[test]
    fn test_delete_parser_without_where() {
        let mut query = String::from("FROM users;");
        let result = delete_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Delete { tables, where_st } = result.unwrap() {
            assert_eq!(tables, ["users"]);
            assert!(where_st.is_none());
        } else {
            panic!("Expected a Delete command");
        }
    }

    #[test]
    fn test_delete_parser_missing_tables() {
        let mut query = String::from("FROM WHERE id = 1;");
        let result = delete_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_parser_incorrect_where_syntax() {
        let mut query = String::from("FROM users WHERE id 1;");
        let result = delete_parser(&mut query);
        assert!(result.is_err());
    }
}
