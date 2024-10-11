use super::utils_commands::{create_table_list, open_csv_reader};
use crate::errors::ErrorType;
use std::{collections::HashMap, fs::OpenOptions, io::Write};

/// Genera un Vector de String con los valores de la fila a insertar
fn get_insert_row(
    headers: &[String],
    values: &[String],
    table_header: &[String],
) -> Result<String, ErrorType> {
    let mut insert_row: Vec<String> = vec![String::new(); table_header.len()];

    // Si no se entregaron headers, pongo los valores en el orden de llegada
    if headers.is_empty() {
        for (i, value) in values.iter().enumerate() {
            if i < insert_row.len() {
                insert_row[i] = value.to_string();
            }
        }
    // Si hay headers especificos, coloca los valores en ese orden
    } else {
        let mut header_index = HashMap::new();
        for (i, header) in table_header.iter().enumerate() {
            header_index.insert(header.as_str(), i);
        }
        for (header, value) in headers.iter().zip(values.iter()) {
            if let Some(&index) = header_index.get(header.as_str()) {
                insert_row[index] = value.to_string();
            } else {
                return Err(ErrorType::InvalidColumn(format!(
                    "Header '{header}' not found in the CSV file"
                )));
            }
        }
    }

    // si el indice quedo vacio inserta string vacio
    for i in insert_row.len()..table_header.len() {
        if insert_row.get(i).is_none() {
            insert_row.push(String::new());
        }
    }
    Ok(insert_row.join(","))
}

/// Verifica que no haya mas valores que headers en la tabla
fn check_insert_headers(values: &[String], table_header: &[String]) -> Result<(), ErrorType> {
    if values.len() > table_header.len() {
        return Err(ErrorType::InvalidColumn(
            "Can't insert more values than the amount of columns".into(),
        ));
    }
    Ok(())
}

pub fn insert(
    tables: &[String],
    headers: &[String],
    values: &[String],
    db_path: &str,
) -> Result<Option<String>, ErrorType> {
    let table_list = create_table_list(tables, db_path)?;

    for table in table_list {
        // abro archivo
        let (_, table_header, _) = open_csv_reader(&table, db_path)
            .map_err(|e| ErrorType::InvalidTable(format!("Error opening table {e}")))?;
        let csv_table = format!("{db_path}/{table}.csv");

        // corroboro headers
        check_insert_headers(values, &table_header)?;

        let insert_row = get_insert_row(headers, values, &table_header)?;

        // abro el archivo en formato append
        let mut file = OpenOptions::new()
            .append(true)
            .open(&csv_table)
            .map_err(|e| ErrorType::InvalidTable(format!("opening file for append {e} failed")))?;

        // escribo la nueva fila en la tabla
        writeln!(file, "{insert_row}")
            .map_err(|e| ErrorType::OtherError(format!("writing to file {e} failed")))?;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;

    fn setup_table(db_path: &str, table_name: &str, headers: &[String]) -> String {
        // Crea una tabla temporal para los tests
        let csv_path = format!("{}/{}.csv", db_path, table_name);
        let mut file = File::create(&csv_path).expect("Unable to create test table file");

        // Escribe los headers en el archivo CSV
        let header_line = headers.join(",");
        writeln!(file, "{}", header_line).expect("Unable to write to test table file");

        csv_path
    }

    fn teardown_table(db_path: &str, table_name: &str) {
        // Elimina el archivo de la tabla después del test
        let csv_path = format!("{}/{}.csv", db_path, table_name);
        if Path::new(&csv_path).exists() {
            fs::remove_file(csv_path).expect("Failed to delete test table file");
        }
    }

    #[test]
    fn test_insert_single_table() {
        let db_path = "./test_insert_db1";
        let table_name = "test_table";
        let headers = ["id".to_string(), "nombre".to_string()];
        let values = ["1".to_string(), "Juan".to_string()];

        // Crear el directorio y la tabla de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        setup_table(db_path, table_name, &headers);

        // Ejecuta el insert
        let tables = vec![table_name.to_string()];
        let insert_result = insert(&tables, &headers, &values, db_path);

        // Verificar que no hubo errores
        assert!(insert_result.is_ok(), "Insert failed: {:?}", insert_result);

        // Verificar que los datos fueron insertados correctamente
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "1,Juan");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_insert_multiple_tables() {
        let db_path = "./test_insert_db2";
        let table_name1 = "test_table_1";
        let table_name2 = "test_table_2";
        let headers = ["id".to_string(), "nombre".to_string()];
        let values = ["2".to_string(), "Maria".to_string()];

        // Crear dos tablas de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        setup_table(db_path, table_name1, &headers);
        setup_table(db_path, table_name2, &headers);

        // Ejecuta el insert en ambas tablas
        let tables = vec![table_name1.to_string(), table_name2.to_string()];
        let insert_result = insert(&tables, &headers, &values, db_path);

        // Verificar que no hubo errores
        assert!(insert_result.is_ok(), "Insert failed: {:?}", insert_result);

        // Verificar que los datos fueron insertados en ambas tablas
        for table_name in &[table_name1, table_name2] {
            let csv_table_path = format!("{}/{}.csv", db_path, table_name);
            let content =
                fs::read_to_string(csv_table_path).expect("Failed to read test table file");
            let lines: Vec<&str> = content.lines().collect();

            assert_eq!(lines.len(), 2);
            assert_eq!(lines[1], "2,Maria");
        }

        teardown_table(db_path, table_name1);
        teardown_table(db_path, table_name2);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_insert_fails_with_missing_table() {
        let db_path = "./test_insert_db3";
        let table_name = "non_existent_table";
        let headers = ["id".to_string(), "nombre".to_string()];
        let values = ["3".to_string(), "Bob".to_string()];

        // No creamos la tabla

        // Ejecuta el insert
        let tables = vec![table_name.to_string()];
        let insert_result = insert(&tables, &headers, &values, db_path);

        // Verificar que el insert falla con el error adecuado
        assert!(insert_result.is_err());
        if let Err(ErrorType::InvalidTable(_)) = insert_result {
            // Error esperado
        } else {
            panic!("Unexpected error type");
        }
    }

    #[test]
    fn test_insert_with_fewer_values() {
        let db_path = "./test_insert_db4";
        let table_name = "test_table";
        let headers = vec!["nombre".to_string(), "age".to_string()];
        let values = vec!["Juan".to_string()]; // Falta el valor para "age"

        // Crear el directorio y la tabla de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        setup_table(db_path, table_name, &headers);

        // Ejecuta el insert
        let tables = vec![table_name.to_string()];
        let result = insert(&tables, &headers, &values, db_path);

        // Verifica que no hubo errores
        assert!(result.is_ok(), "Insert failed: {:?}", result);

        // Verifica que "Juan" se insertó y que "age" está vacío
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "Juan,");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_insert_with_extra_values() {
        let db_path = "./test_insert_db5";
        let table_name = "test_table";
        let headers = vec!["nombre".to_string()];
        let values = vec!["Juan".to_string(), "25".to_string()]; // Un valor extra

        // Crear el directorio y la tabla de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        setup_table(db_path, table_name, &headers);

        // Ejecuta el insert
        let tables = vec![table_name.to_string()];
        let result = insert(&tables, &headers, &values, db_path);

        // Verifica que hubo error
        assert!(result.is_err());

        if let Err(ErrorType::InvalidColumn(err)) = result {
            assert_eq!(err, "Can't insert more values than the amount of columns");
        } else {
            panic!("Expected InvalidColumn error");
        }

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_insert_with_exact_values() {
        let db_path = "./test_insert_db6";
        let table_name = "test_table";
        let headers = vec!["nombre".to_string(), "age".to_string()];
        let values = vec!["Juan".to_string(), "25".to_string()];

        // Crear el directorio y la tabla de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        setup_table(db_path, table_name, &headers);

        // Ejecuta el insert
        let tables = vec![table_name.to_string()];
        let result = insert(&tables, &headers, &values, db_path);

        // Verifica que no hubo errores
        assert!(result.is_ok(), "Insert failed: {:?}", result);

        // Verifica que se insertó correctamente con los valores proporcionados
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "Juan,25");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_insert_with_empty_headers() {
        let db_path = "./test_insert_db7";
        let table_name = "test_table";
        let table_headers = vec!["id".to_string(), "nombre".to_string()];
        let values = vec!["1".to_string(), "Juan".to_string()];

        // Crear el directorio y la tabla de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        setup_table(db_path, table_name, &table_headers);

        // Ejecuta el insert con headers vacíos
        let tables = vec![table_name.to_string()];
        let insert_result = insert(&tables, &[], &values, db_path);

        // Verificar que no hubo errores
        assert!(insert_result.is_ok(), "Insert failed: {:?}", insert_result);

        // Verificar que los datos fueron insertados correctamente
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "1,Juan");

        // Teardown: Elimina la tabla de prueba y el directorio
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_insert_multiple_tables_with_asterisk() {
        let db_path = "./test_insert_db8";
        let table_names = vec!["test_table1", "test_table2"];
        let table_headers = vec!["id".to_string(), "nombre".to_string()];
        let values = vec!["1".to_string(), "Juan".to_string()];

        // Crear el directorio y las tablas de prueba
        fs::create_dir_all(db_path).expect("Failed to create test database directory");
        for table_name in &table_names {
            setup_table(db_path, table_name, &table_headers);
        }

        // Ejecuta el insert en todas las tablas con el wildcard "*"
        let tables = vec!["*".to_string()];
        let insert_result = insert(&tables, &[], &values, db_path);

        // Verificar que no hubo errores
        assert!(insert_result.is_ok(), "Insert failed: {:?}", insert_result);

        // Verificar que los datos fueron insertados correctamente en ambas tablas
        for table_name in &table_names {
            let csv_table_path = format!("{}/{}.csv", db_path, table_name);
            let content =
                fs::read_to_string(csv_table_path).expect("Failed to read test table file");
            let lines: Vec<&str> = content.lines().collect();

            assert_eq!(lines.len(), 2, "Table '{}' should have 2 lines", table_name);
            assert_eq!(
                lines[1], "1,Juan",
                "Incorrect data in table '{}'",
                table_name
            );
        }

        for table_name in &table_names {
            teardown_table(db_path, table_name);
        }
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
}
