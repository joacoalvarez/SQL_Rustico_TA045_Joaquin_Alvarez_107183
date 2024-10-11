use crate::errors::ErrorType;
use crate::condition::Condition;
use std::io::BufReader;
use std::{collections::HashMap, error::Error, fs, fs::File, io, io::BufRead, io::Write};

type CsvReader = BufReader<File>;
type Header = Vec<String>;
type HeaderIndex = HashMap<String, usize>;
type OpenCsvReaderResult = Result<(CsvReader, Header, HeaderIndex), Box<dyn Error>>;
type CreateAuxFileResult = Result<(File, String, String), Box<dyn Error>>;

/// Obtiene los headers de la tabla
fn get_header<R: BufRead>(reader: &mut R) -> io::Result<Vec<String>> {
    let mut first_line = String::new();

    reader.read_line(&mut first_line)?;

    first_line = first_line.trim_end().to_string();

    let header: Vec<String> = first_line
        .split(',')
        .map(|x| x.trim().to_string())
        .collect();

    Ok(header)
}

/// Crea un reader, lo devuelve junto a un Vector con los headers y un `HashMap` con la posicion de cada header
pub fn open_csv_reader(table: &String, db_path: &str) -> OpenCsvReaderResult {
    let csv_table = format!("{db_path}/{table}.csv");
    let file = File::open(&csv_table)?;
    let mut reader = BufReader::new(file);

    let table_header = get_header(&mut reader)?;

    let mut header_index = HashMap::new();
    for (i, header) in table_header.iter().enumerate() {
        header_index.insert(header.to_string(), i);
    }

    Ok((reader, table_header, header_index))
}

/// Crea un archivo auxiliar para guardar los datos mientras se controlan las condiciones, agrega el header de la tabla
pub fn create_aux_file(
    table: &String,
    db_path: &str,
    table_header: &[String],
) -> CreateAuxFileResult {
    let aux_table = format!("{db_path}/{table}_temp.csv");
    let aux_file = File::create(&aux_table)?;

    writeln!(&aux_file, "{}", table_header.join(","))?;

    Ok((aux_file, format!("{db_path}/{table}.csv"), aux_table))
}

/// Crea un `HashMap` dado una linea con valores de la tabla y los headers de la misma
pub fn create_row_values_map(table_header: &[String], buffer: &str) -> HashMap<String, String> {
    let row: Vec<String> = buffer.trim().split(',').map(ToString::to_string).collect();

    let mut row_values_map: HashMap<String, String> = HashMap::new();
    for (i, value) in row.iter().enumerate() {
        if let Some(header) = table_header.get(i) {
            row_values_map.insert(header.to_string(), value.to_string());
        }
    }
    row_values_map
}

/// Crea una lista con las tablas a efectuar los comandos
pub fn create_table_list(tables: &[String], db_path: &str) -> Result<Vec<String>, ErrorType> {
    if tables.len() == 1 && tables[0] == "*" {
        // Si el vector contiene un solo "*", devolver todas las tablas del path
        get_all_tables(db_path)
    } else {
        // En caso contrario, devolver el vector original
        Ok(tables.to_vec())
    }
}

/// Devuelve todas las tablas csv dentro de un directorio
fn get_all_tables(db_path: &str) -> Result<Vec<String>, ErrorType> {
    let mut tables = Vec::new();
    let paths = fs::read_dir(db_path)
        .map_err(|e| ErrorType::OtherError(format!("reading directory failed: {e}")))?;

    for path in paths {
        let path = path.map_err(|e| ErrorType::OtherError(format!("reading path failed: {e}")))?;
        if let Some(extension) = path.path().extension() {
            if extension == "csv" {
                if let Some(file_name) = path.path().file_stem() {
                    if let Some(file_name_str) = file_name.to_str() {
                        tables.push(file_name_str.to_string());
                    } else {
                        return Err(ErrorType::OtherError(
                            "Error converting file name to string".to_string(),
                        ));
                    }
                }
            }
        }
    }

    if tables.is_empty() {
        return Err(ErrorType::InvalidTable(
            "No tables found in the directory".to_string(),
        ));
    }

    Ok(tables)
}

/// Verifica si pasa las condiciones la fila
pub fn should_filter(where_st: &Option<Condition>, row_values_map: &HashMap<String, String>) -> bool {
    match where_st {
        Some(cond) => cond.evaluate(row_values_map),
        None => true,
    }
}