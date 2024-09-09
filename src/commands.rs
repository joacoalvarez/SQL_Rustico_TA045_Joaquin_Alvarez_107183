use crate::command_types::Commands;
use crate::condition::Condition;
use crate::order::{OrderBy, OrderDirection};
use std::collections::HashSet;
use std::io::BufReader;
use std::{
    collections::HashMap, error::Error, fs::File, fs::OpenOptions, io, io::BufRead, io::Write,
};

type CsvReader = BufReader<File>;
type Header = Vec<String>;
type HeaderIndex = HashMap<String, usize>;
type OpenCsvReaderResult = Result<(CsvReader, Header, HeaderIndex), Box<dyn Error>>;
type CreateAuxFileResult = Result<(File, String, String), Box<dyn Error>>;

pub fn execute(command: &Commands, db_path: &str) -> Result<(), Box<dyn Error>> {
    match command {
        Commands::Insert {
            table,
            headers,
            values,
        } => insert(table, headers, values, db_path),
        Commands::Update {
            table,
            updates,
            where_st,
        } => update(table, updates, where_st, db_path),
        Commands::Delete { table, where_st } => delete(table, where_st, db_path),
        Commands::Select {
            headers,
            table,
            where_st,
            order,
        } => select(headers, table, where_st, order, db_path),
    }
}

fn insert(
    table: &String,
    headers: &[String],
    values: &[String],
    db_path: &str,
) -> Result<(), Box<dyn Error>> {
    // abro archivo
    let (_, table_header, _) = open_csv_reader(table, db_path)?;
    let csv_table = format!("{}/{}.csv", db_path, table);

    // corroboro headers
    if values.len() > table_header.len() {
        return Err("Error: Can't insert more values than the amount of columns".into());
    }
    let mut insert_line = vec![String::new(); table_header.len()];
    if headers.is_empty() {
        for (i, value) in values.iter().enumerate() {
            if i < insert_line.len() {
                insert_line[i] = value.to_string();
            }
        }
    } else {
        let mut header_index = HashMap::new();
        for (i, header) in table_header.iter().enumerate() {
            header_index.insert(header.as_str(), i);
        }
        for (header, value) in headers.iter().zip(values.iter()) {
            if let Some(&index) = header_index.get(header.as_str()) {
                insert_line[index] = value.to_string();
            } else {
                return Err(format!("Header '{}' not found in the CSV file", header).into());
            }
        }
    }

    // creo nueva linea y la adjunto al archivo original
    for _ in insert_line.len()..table_header.len() {
        insert_line.push(String::new());
    }

    let mut file = OpenOptions::new().append(true).open(&csv_table)?;
    let insert_line = insert_line
        .iter()
        .map(|x| x.as_str())
        .collect::<Vec<&str>>()
        .join(",");
    writeln!(file, "{}", insert_line)?;
    Ok(())
}

fn update(
    table: &String,
    updates: &HashMap<String, String>,
    where_st: &Option<Condition>,
    db_path: &str,
) -> Result<(), Box<dyn Error>> {
    // abro archivo y auxiliar
    let (mut reader, table_header, header_index) = open_csv_reader(table, db_path)?;

    let (mut aux_file, csv_table, aux_table) = create_aux_file(table, db_path, &table_header)?;

    for key in updates.keys() {
        if !header_index.contains_key(key.as_str()) {
            return Err(format!("Column to update '{}' not found in the table", key).into());
        }
    }

    // leo linea a linea y corroboro si cumple el where
    let mut buffer = String::new();
    while reader.read_line(&mut buffer)? > 0 {
        let row: Vec<String> = buffer
            .trim_end()
            .split(',')
            .map(|s| s.to_string())
            .collect();
        buffer.clear();

        let mut row_values_map: HashMap<String, String> = HashMap::new();
        for (i, value) in row.iter().enumerate() {
            if let Some(header) = table_header.get(i) {
                row_values_map.insert(header.to_string(), value.to_string());
            }
        }

        // evaluo la fila por la condicion where
        let should_update = match where_st {
            Some(cond) => cond.evaluate(&row_values_map),
            None => true,
        };

        // rearmo la fila con los valores actualizados
        let updated_row: Vec<String> = table_header
            .iter()
            .map(|header| {
                if should_update {
                    updates
                        .get(header.as_str())
                        .unwrap_or(&row_values_map[header.as_str()])
                        .to_string()
                } else {
                    row_values_map
                        .get(header.as_str())
                        .unwrap_or(&"".to_string())
                        .to_string()
                }
            })
            .collect();

        writeln!(aux_file, "{}", updated_row.join(","))?;
    }

    std::fs::rename(&aux_table, &csv_table)?;
    Ok(())
}

fn delete(
    table: &String,
    where_st: &Option<Condition>,
    db_path: &str,
) -> Result<(), Box<dyn Error>> {
    // abro archivo y auxiliar
    let (mut reader, table_header, _) = open_csv_reader(table, db_path)?;

    let (mut aux_file, csv_table, aux_table) = create_aux_file(table, db_path, &table_header)?;

    // evaluo la fila por la condicion where
    let mut buffer = String::new();
    while reader.read_line(&mut buffer)? > 0 {
        let row: Vec<String> = buffer
            .trim_end()
            .split(',')
            .map(|s| s.to_string())
            .collect();
        buffer.clear();

        let mut row_values_map: HashMap<String, String> = HashMap::new();
        for (i, value) in row.iter().enumerate() {
            if let Some(header) = table_header.get(i) {
                row_values_map.insert(header.to_string(), value.to_string());
            }
        }
        let should_delete = match where_st {
            Some(cond) => cond.evaluate(&row_values_map),
            None => false,
        };

        // si no hay que borrar lo escribo en el auxiliar
        if !should_delete {
            writeln!(aux_file, "{}", row.join(","))?;
        }
    }

    std::fs::rename(&aux_table, &csv_table)?;
    Ok(())
}

fn select(
    headers: &[String],
    table: &String,
    where_st: &Option<Condition>,
    order: &Option<Vec<OrderBy>>,
    db_path: &str,
) -> Result<(), Box<dyn Error>> {
    // abro archivo y auxiliar
    let (mut reader, table_header, _) = open_csv_reader(table, db_path)?;
    //seteo los headers elegidos
    let selected_headers: HashSet<&str> = headers.iter().map(AsRef::as_ref).collect();

    //elimino los headers no elegidos del index para posterior filtro en where
    let header_index: HashMap<&str, usize> = table_header
        .iter()
        .enumerate()
        .filter_map(|(i, header)| {
            if selected_headers.contains(header.as_str()) {
                Some((header.as_str(), i))
            } else {
                None
            }
        })
        .collect();

    for header in &selected_headers {
        if !header_index.contains_key(header) {
            return Err(format!("Column to select '{}' not found in the table", header).into());
        }
    }

    // filtro el where, dejo los headers selccionados
    let mut selected_rows: Vec<Vec<String>> = Vec::new();
    let mut buffer = String::new();
    while reader.read_line(&mut buffer)? > 0 {
        let row: Vec<String> = buffer
            .trim_end()
            .split(',')
            .map(|s| s.to_string())
            .collect();
        buffer.clear();

        let mut row_map: HashMap<String, String> = HashMap::new();
        for (i, value) in row.iter().enumerate() {
            if let Some(header) = table_header.get(i) {
                row_map.insert(header.to_string(), value.to_string());
            }
        }

        let should_select = match where_st {
            Some(cond) => cond.evaluate(&row_map),
            None => true,
        };

        if should_select {
            let selected_row: Vec<String> = header_index
                .values()
                .map(|&i| row.get(i).unwrap_or(&String::new()).to_string())
                .collect();
            selected_rows.push(selected_row);
        }
    }

    // con los seleccionados del where ordeno segun criterio pedido
    if let Some(criteras) = order {
        selected_rows.sort_by(|a, b| {
            for OrderBy { column, direction } in criteras {
                if let Some(idx) = headers.iter().position(|header| header == column) {
                    if idx < a.len() && idx < b.len() {
                        let value_a = &a[idx];
                        let value_b = &b[idx];

                        let cmp = value_a.cmp(value_b);
                        if cmp != std::cmp::Ordering::Equal {
                            return match direction {
                                OrderDirection::Ascending => cmp,
                                OrderDirection::Descending => cmp.reverse(),
                            };
                        }
                    }
                } else {
                    panic!("Column '{}' not found in headers", column);
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    for line in selected_rows {
        println!("{}", line.join(","));
    }
    Ok(())
}

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

fn open_csv_reader(table: &String, db_path: &str) -> OpenCsvReaderResult {
    let csv_table = format!("{}/{}.csv", db_path, table);
    let file = File::open(&csv_table)?;
    let mut reader = BufReader::new(file);

    let table_header = get_header(&mut reader)?;

    let mut header_index = HashMap::new();
    for (i, header) in table_header.iter().enumerate() {
        header_index.insert(header.to_string(), i);
    }

    Ok((reader, table_header, header_index))
}

fn create_aux_file(table: &String, db_path: &str, table_header: &[String]) -> CreateAuxFileResult {
    let aux_table = format!("{}/{}_temp.csv", db_path, table);
    let aux_file = File::create(&aux_table)?;

    writeln!(&aux_file, "{}", table_header.join(","))?;

    Ok((aux_file, format!("{}/{}.csv", db_path, table), aux_table))
}
