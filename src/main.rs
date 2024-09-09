// Declaro modulos
mod command_types;
mod commands;
mod condition;
mod extract_query;
mod order;
mod parser;

use commands::execute;
use parser::parse_query;
use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // leo argumentos y chequeo que hayan mas de uno
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        return Err(Box::from("Error: The Program needs at least 3 arguments"));
    }

    let db_path: &str = &args[1];
    let query: &str = &args[2];

    let command_result = parse_query(&mut query.to_string());

    match command_result {
        Ok(command) => {
            // Execute the command and handle the result
            execute(&command, db_path).map_err(|e| Box::from(format!("Execution Error: {}", e)))
        }
        Err(e) => Err(Box::from(format!("INVALID_SYNTAX: {}", e))),
    }
}
