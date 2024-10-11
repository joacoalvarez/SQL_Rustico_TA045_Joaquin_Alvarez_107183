mod delete;
mod insert;
mod select;
mod update;
mod utils_commands;

use delete::delete;
use insert::insert;
use select::select;
use update::update;

use crate::command_types::Commands;
use crate::errors::ErrorType;

/// Ejecuta el comando
#[allow(clippy::missing_errors_doc)]
pub fn execute(command: &Commands, db_path: &str) -> Result<Option<String>, ErrorType> {
    match command {
        Commands::Insert {
            tables,
            headers,
            values,
        } => insert(tables, headers, values, db_path),
        Commands::Update {
            tables,
            updates,
            where_st,
        } => update(tables, updates, where_st, db_path),
        Commands::Delete { tables, where_st } => delete(tables, where_st, db_path),
        Commands::Select {
            headers,
            tables,
            where_st,
            order,
        } => select(headers, tables, where_st, order, db_path),
    }
}
