// Declaro modulos
mod command_types;
mod commands;
mod condition;
mod errors;
mod order;
mod parser;

use command_types::Commands;
use commands::execute;
use errors::ErrorType;
use parser::parse_query;
use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Leo argumentos y chequeo que hayan mas de uno
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        return Err(
            ErrorType::OtherError("The program needs at least 3 arguments".into()).create_error(),
        );
    }

    let db_path: &str = &args[1];
    let query: &str = &args[2];

    // parseo la query, chequeo errores y obtengo el comando
    let command: Commands = parse_query(&mut query.to_string()).map_err(|e| e.create_error())?;

    // ejecuto el comando y busco errores
    execute(&command, db_path).map_err(|e| e.create_error())?;

    Ok(())
}
