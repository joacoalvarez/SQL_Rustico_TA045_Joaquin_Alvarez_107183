use crate::command_types::Commands;
use crate::condition::Condition;
use crate::extract_query::{
    extract_and_parse_order_clause, extract_and_parse_where_clause, extract_between_parenthesis,
    extract_next_word, extract_select_headers, extract_updates, get_next_char,
};
use crate::order::OrderBy;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;

pub fn parse_query(query: &mut String) -> Result<Commands, Box<dyn Error>> {
    // si no tiene end of query (;) se lo agrego
    if !query.trim_end().ends_with(';') {
        query.push(';')
    }

    let command = extract_next_word(query).ok_or("No command found")?;
    match Commands::from_str(&command) {
        Ok(Commands::Insert { .. }) => insert_parser(query),
        Ok(Commands::Update { .. }) => update_parser(query),
        Ok(Commands::Delete { .. }) => delete_parser(query),
        Ok(Commands::Select { .. }) => select_parser(query),
        Err(e) => Err(Box::from(e)),
    }
}

fn insert_parser(query: &mut String) -> Result<Commands, Box<dyn Error>> {
    let table: String;
    let headers: Vec<String>;
    let values: Vec<String>;

    // busco el INTO
    match extract_next_word(query) {
        Some(word) if word == "INTO" => {
            // busco la tabla
            table = match extract_next_word(query) {
                Some(table_name) => table_name,
                None => return Err("Missing table name".into()),
            };
            //busco si hay headers
            if let Some('(') = get_next_char(query) {
                headers = extract_between_parenthesis(query)?;
            } else {
                headers = Vec::new()
            }
            match extract_next_word(query) {
                // busco los values
                Some(word) if word == "VALUES" => {
                    values = extract_between_parenthesis(query)?;
                }
                Some(word) => {
                    return Err(
                        format!("Expected 'VALUES' after the headers, found '{}'", word).into(),
                    )
                }
                None => return Err("Expected 'VALUES' after the headers, found nothing".into()),
            }
        }
        Some(word) => {
            return Err(format!("Expected 'INTO' after INSERT command, found '{}'", word).into())
        }
        None => return Err("Expected 'INTO' after 'INSERT' command, found nothing".into()),
    }

    Ok(Commands::Insert {
        table,
        headers,
        values,
    })
}

fn update_parser(query: &mut String) -> Result<Commands, Box<dyn Error>> {
    let updates: HashMap<String, String>;
    let where_st: Option<Condition>;

    let table = match extract_next_word(query) {
        Some(table_name) => table_name,
        None => return Err("Missing table name".into()),
    };

    //busco los sets
    match extract_next_word(query) {
        Some(word) if word == "SET" => {
            updates = extract_updates(query)?;
        }
        Some(word) => {
            return Err(format!("Expected 'SET' after 'UPDATE' command, found '{}'", word).into())
        }
        None => return Err("Expected 'SET' after 'UPDATE' command, found nothing".into()),
    }

    match get_next_char(query) {
        // si esta ; no hay where
        Some(';') => {
            *query = query.trim_start_matches(';').to_string();
            where_st = None;
        }
        // manejo where o palabra desconocida
        _ => match extract_next_word(query) {
            Some(word) if word == "WHERE" => {
                where_st = Some(extract_and_parse_where_clause(query)?);
            }
            None => where_st = None,
            Some(word) => {
                return Err(format!("Expected WHERE or end of query, found '{}'", word).into());
            }
        },
    }

    Ok(Commands::Update {
        table,
        updates,
        where_st,
    })
}

fn delete_parser(query: &mut String) -> Result<Commands, Box<dyn Error>> {
    match extract_next_word(query) {
        Some(word) if word == "FROM" => {}
        _ => return Err("Expected 'FROM' keyword after 'DELETE'".into()),
    }

    let table = match extract_next_word(query) {
        Some(table_name) => table_name,
        None => return Err("Missing table name".into()),
    };

    let where_st: Option<Condition> = match extract_next_word(query) {
        // busco si hay where o se termino la query
        Some(word) if word == "WHERE" => {
            //implementacion where
            Some(extract_and_parse_where_clause(query)?)
        }
        Some(word) if word == ";" => None,
        None => None,
        Some(_) => return Err("Expected WHERE or end of query, found something else".into()),
    };

    Ok(Commands::Delete { table, where_st })
}

fn select_parser(query: &mut String) -> Result<Commands, Box<dyn Error>> {
    let mut where_st: Option<Condition> = None;
    let mut order: Option<Vec<OrderBy>> = None;

    let headers = extract_select_headers(query)?;
    match extract_next_word(query) {
        Some(word) if word == "FROM" => {}
        _ => return Err("Expected 'FROM' keyword after headers".into()),
    }
    let table = match extract_next_word(query) {
        Some(table_name) => table_name,
        None => return Err("Missing table name".into()),
    };

    while let Some(word) = extract_next_word(query) {
        // busco si hay where o se termino la query
        match word.as_str() {
            "WHERE" => {
                //implementacion where
                where_st = Some(extract_and_parse_where_clause(query)?);
            }
            "ORDER" => {
                order = Some(extract_and_parse_order_clause(query)?);
            }
            ";" => {
                break;
            }

            _ => {
                return Err(format!(
                    "Expected 'WHERE', 'ORDER BY', or end of query. found {}",
                    word
                )
                .into())
            }
        }
    }

    Ok(Commands::Select {
        headers,
        table,
        where_st,
        order,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_types::Commands;

    #[test]
    fn test_insert_parser_with_headers() {
        let mut query = String::from("INTO users (id, name) VALUES (1, 'Alice');");
        let result = insert_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Insert {
            table,
            headers,
            values,
        } = result.unwrap()
        {
            assert_eq!(table, "users");
            assert_eq!(headers, vec!["id", "name"]);
            assert_eq!(values, vec!["1", "Alice"]);
        } else {
            panic!("Expected an Insert command");
        }
    }

    #[test]
    fn test_insert_parser_without_headers() {
        let mut query = String::from("INTO users VALUES (1, 'Alice');");
        let result = insert_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Insert {
            table,
            headers,
            values,
        } = result.unwrap()
        {
            assert_eq!(table, "users");
            assert!(headers.is_empty());
            assert_eq!(values, vec!["1", "Alice"]);
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
    fn test_insert_parser_missing_table() {
        let mut query = String::from("INTO (id, name) VALUES (1, 'Alice');");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_parser_missing_values_keyword() {
        let mut query = String::from("INTO users (id, name) (1, 'Alice');");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_parser_missing_parentheses() {
        let mut query = String::from("INTO users (id, name) VALUES 1, 'Alice';");
        let result = insert_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_parser_with_where() {
        let mut query = String::from("users SET name = 'Alice' WHERE id = 1;");
        let result = update_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Update {
            table,
            updates,
            where_st,
        } = result.unwrap()
        {
            assert_eq!(table, "users");
            assert_eq!(updates.get("name"), Some(&"Alice".to_string()));
            assert!(where_st.is_some());
        } else {
            panic!("Expected an Update command");
        }
    }

    #[test]
    fn test_update_parser_without_where() {
        let mut query = String::from("users SET name = 'Alice';");
        let result = update_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Update {
            table,
            updates,
            where_st,
        } = result.unwrap()
        {
            assert_eq!(table, "users");
            assert_eq!(updates.get("name"), Some(&"Alice".to_string()));
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
        let mut query = String::from("users name = 'Alice';");
        let result = update_parser(&mut query);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_parser_incorrect_update_syntax() {
        let mut query = String::from("users SET name 'Alice';");
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
            table,
            updates,
            where_st,
        } = result.unwrap()
        {
            assert_eq!(table, "users");
            assert_eq!(updates.get("name"), Some(&"".to_string()));
            assert!(where_st.is_some());
        } else {
            panic!("Expected an Update command");
        }
    }

    #[test]
    fn test_delete_parser_with_where() {
        let mut query = String::from("FROM users WHERE id = 1;");
        let result = delete_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Delete { table, where_st } = result.unwrap() {
            assert_eq!(table, "users");
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

        if let Commands::Delete { table, where_st } = result.unwrap() {
            assert_eq!(table, "users");
            assert!(where_st.is_none());
        } else {
            panic!("Expected a Delete command");
        }
    }

    #[test]
    fn test_delete_parser_missing_table() {
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

    #[test]
    fn test_select_parser_with_where_and_order() {
        let mut query = String::from("name, age FROM users WHERE age > 18 ORDER BY age DESC;");
        let result = select_parser(&mut query);
        assert!(result.is_ok());

        if let Commands::Select {
            headers,
            table,
            where_st,
            order,
        } = result.unwrap()
        {
            assert_eq!(headers, vec!["name", "age"]);
            assert_eq!(table, "users");
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
            table,
            where_st,
            order,
        } = result.unwrap()
        {
            assert_eq!(headers, vec!["name", "age"]);
            assert_eq!(table, "users");
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
