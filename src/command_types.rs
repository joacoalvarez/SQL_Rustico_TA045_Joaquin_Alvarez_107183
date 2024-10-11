use crate::{condition::Condition, order::OrderBy};
use std::{collections::HashMap, str::FromStr};

/// Representacion de los distintos comandos
#[derive(Debug)]
pub enum Commands {
    Insert {
        tables: Vec<String>,
        headers: Vec<String>,
        values: Vec<String>,
    },
    Update {
        tables: Vec<String>,
        updates: HashMap<String, String>,
        where_st: Option<Condition>,
    },
    Delete {
        tables: Vec<String>,
        where_st: Option<Condition>,
    },
    Select {
        headers: Vec<String>,
        tables: Vec<String>,
        where_st: Option<Condition>,
        order: Option<Vec<OrderBy>>,
    },
}

/// Recibe un string y retorna su correspondiente Commmand inicializado
impl FromStr for Commands {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INSERT" => Ok(Commands::Insert {
                tables: Vec::new(),
                headers: Vec::new(),
                values: Vec::new(),
            }),
            "UPDATE" => Ok(Commands::Update {
                tables: Vec::new(),
                updates: HashMap::new(),
                where_st: None,
            }),
            "DELETE" => Ok(Commands::Delete {
                tables: Vec::new(),
                where_st: None,
            }),
            "SELECT" => Ok(Commands::Select {
                headers: Vec::new(),
                tables: Vec::new(),
                where_st: None,
                order: None,
            }),
            _ => {
                Err("INVALID_SYNTAX: The Query Command doesn't match any of the available options")
            }
        }
    }
}
