mod extract_query;
mod parse_delete;
mod parse_insert;
mod parse_select;
mod parse_update;

use parse_delete::delete_parser;
use parse_insert::insert_parser;
use parse_select::select_parser;
use parse_update::update_parser;

use crate::command_types::Commands;
use crate::errors::ErrorType;
use extract_query::extract_next_word;
use std::str::FromStr;

/// Parsea la query y devuelve el `Commands` inicializado o el `ErrorType` correspondiente
#[allow(clippy::missing_errors_doc)]
pub fn parse_query(query: &mut String) -> Result<Commands, ErrorType> {
    // si no tiene end of query (;) se lo agrego
    if !query.trim_end().ends_with(';') {
        query.push(';');
    }

    let command =
        extract_next_word(query).ok_or(ErrorType::InvalidSyntax("No command found".to_string()))?;
    match Commands::from_str(&command) {
        Ok(Commands::Insert { .. }) => insert_parser(query),
        Ok(Commands::Update { .. }) => update_parser(query),
        Ok(Commands::Delete { .. }) => delete_parser(query),
        Ok(Commands::Select { .. }) => select_parser(query),
        Err(e) => Err(ErrorType::InvalidSyntax(e.to_string())),
    }
}
