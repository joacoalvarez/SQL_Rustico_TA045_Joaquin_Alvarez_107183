use crate::{condition::Condition, order::OrderBy};
use std::{collections::HashMap, str::FromStr};

// Enum representing the different SQL commands.
#[derive(Debug)]
pub enum Commands {
    Insert {
        table: String,
        headers: Vec<String>,
        values: Vec<String>,
    },
    Update {
        table: String,
        updates: HashMap<String, String>,
        where_st: Option<Condition>,
    },
    Delete {
        table: String,
        where_st: Option<Condition>,
    },
    Select {
        headers: Vec<String>,
        table: String,
        where_st: Option<Condition>,
        order: Option<Vec<OrderBy>>,
    },
}

//Implement FromStr to allow parsing commands from strings.
impl FromStr for Commands {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INSERT" => Ok(Commands::Insert {
                table: String::new(),
                headers: Vec::new(),
                values: Vec::new(),
            }),
            "UPDATE" => Ok(Commands::Update {
                table: String::new(),
                updates: HashMap::new(),
                where_st: None,
            }),
            "DELETE" => Ok(Commands::Delete {
                table: String::new(),
                where_st: None,
            }),
            "SELECT" => Ok(Commands::Select {
                headers: Vec::new(),
                table: String::new(),
                where_st: None,
                order: None,
            }),
            _ => {
                Err("INVALID_SYNTAX: The Query Command doesn't match any of the available options")
            }
        }
    }
}
