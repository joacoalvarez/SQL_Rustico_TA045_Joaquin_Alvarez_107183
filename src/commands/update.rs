use super::utils_commands::{
    create_aux_file, create_row_values_map, create_table_list, open_csv_reader, should_filter,
};
use crate::condition::Condition;
use crate::errors::ErrorType;
use std::{
    collections::HashMap,
    io::{BufRead, Write},
    fs::File
};

/// Verifica que los headers a actualizar esten en la tabla
fn check_update_headers(
    updates: &HashMap<String, String>,
    header_index: &HashMap<String, usize>,
) -> Result<(), ErrorType> {
    for key in updates.keys() {
        if !header_index.contains_key(key.as_str()) {
            return Err(ErrorType::InvalidColumn(format!(
                "Column to update '{key}' not found in the table"
            )));
        }
    }

    Ok(())
}

/// Actualiza los valores de la fila
fn update_row_values(
    table_header: &[String],
    row_values_map: &HashMap<String, String>,
    updates: &HashMap<String, String>,
) -> Vec<String> {
    table_header
        .iter()
        .map(|header| {
            updates
                .get(header.as_str()) // Obtener el valor actualizado si existe
                .unwrap_or(&row_values_map[header.as_str()]) // Si no, usar el valor original
                .to_string()
        })
        .collect()
}

// Rearma la fila con los valores actualizados de haber, sino reuso los del buffer
pub fn write_update(
    aux_file: &mut File,
    should_update: bool,
    table_header: &[String],
    row_values_map: &HashMap<String, String>,
    updates: &HashMap<String, String>,
    buffer: &str,
) -> Result<(), ErrorType> {
    if should_update {
        let updated_row = update_row_values(table_header, row_values_map, updates);
        writeln!(aux_file, "{}", updated_row.join(",")).map_err(|e| {
            ErrorType::OtherError(format!("writing to auxiliary file {e} failed"))
        })?;
    } else {
        writeln!(aux_file, "{}", buffer.trim()).map_err(|e| {
            ErrorType::OtherError(format!("writing to auxiliary file {e} failed"))
        })?;
    }
    Ok(())
}

pub fn update(
    tables: &[String],
    updates: &HashMap<String, String>,
    where_st: &Option<Condition>,
    db_path: &str,
) -> Result<Option<String>, ErrorType> {
    let table_list = create_table_list(tables, db_path)?;

    for table in table_list {
        // abro archivo y auxiliar
        let (mut reader, table_header, header_index) = open_csv_reader(&table, db_path)
            .map_err(|e| ErrorType::InvalidTable(format!("opening table {e} failed")))?;

        let (mut aux_file, csv_table, aux_table) = create_aux_file(&table, db_path, &table_header)
            .map_err(|e| ErrorType::OtherError(format!("creating auxiliary file {e} failed")))?;

        // chequeo que las columnas a actualizar esten en la tabla
        check_update_headers(updates, &header_index)?;

        // leo linea a linea y corroboro si cumple el where
        let mut buffer = String::new();
        while reader
            .read_line(&mut buffer)
            .map_err(|e| ErrorType::OtherError(format!("Error reading line: {e}")))?
            > 0
        {
            let row_values_map = create_row_values_map(&table_header, &buffer);

            // evaluo la fila por la condicion where
            let should_update = should_filter(where_st, &row_values_map);

            // escribe en el archivo auxiliar la fila actualizada
            write_update(&mut aux_file, should_update, &table_header, &row_values_map, updates, &buffer)?;

            buffer.clear(); // Limpiar buffer para la próxima línea
        }

        std::fs::rename(&aux_table, &csv_table)
            .map_err(|e| ErrorType::OtherError(format!("renaming file {e} failed")))?;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::ComparisonOp;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;

    fn setup_table(
        db_path: &str,
        table_name: &str,
        headers: &[String],
        initial_values: Option<&[String]>,
    ) -> String {
        // Crea una tabla temporal para los tests
        let csv_path = format!("{}/{}.csv", db_path, table_name);
        let mut file = File::create(&csv_path).expect("Unable to create test table file");

        // Escribe los headers en el archivo CSV
        let header_line = headers.join(",");
        writeln!(file, "{}", header_line).expect("Unable to write to test table file");

        // Si se proporcionan valores iniciales, los escribe también
        if let Some(values) = initial_values {
            writeln!(file, "{}", values.join(","))
                .expect("Unable to write initial values to test table file");
        }

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
    fn test_update_single_table_with_exact_values_1() {
        let db_path = "./test_update_db1";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );

        let mut update_data = HashMap::new();
        update_data.insert("nombre".to_string(), "Juan".to_string());

        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &update_data, &where_cond, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "1,Juan");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_single_table_with_no_condition() {
        let db_path = "./test_update_db2";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );

        let mut update_data = HashMap::new();
        update_data.insert("nombre".to_string(), "Juan".to_string());

        let where_cond = None; // Sin condición

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &update_data, &where_cond, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "1,Juan"); // Se actualiza sin condición

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_single_table_with_non_existing_condition() {
        let db_path = "./test_update_db3";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );

        let mut update_data = HashMap::new();
        update_data.insert("nombre".to_string(), "Juan".to_string());

        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "999".to_string(),
        )); // No existe el id

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &update_data, &where_cond, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "1,Carlos"); // No debe cambiar porque la condición no se cumple

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_multiple_tables_with_exact_values() {
        let db_path = "./test_update_db4";
        let table_name1 = "test_table1";
        let table_name2 = "test_table2";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name1,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );
        setup_table(
            db_path,
            table_name2,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["2".to_string(), "Maria".to_string()]),
        );

        let mut update_data = HashMap::new();
        update_data.insert("nombre".to_string(), "Juan".to_string());

        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name1.to_string(), table_name2.to_string()];
        let update_result = update(&tables, &update_data, &where_cond, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        // Verificar la primera tabla
        let csv_table_path1 = format!("{}/{}.csv", db_path, table_name1);
        let content1 = fs::read_to_string(csv_table_path1).expect("Failed to read test table file");
        let lines1: Vec<&str> = content1.lines().collect();
        assert_eq!(lines1.len(), 2);
        assert_eq!(lines1[1], "1,Juan");

        // Verificar la segunda tabla
        let csv_table_path2 = format!("{}/{}.csv", db_path, table_name2);
        let content2 = fs::read_to_string(csv_table_path2).expect("Failed to read test table file");
        let lines2: Vec<&str> = content2.lines().collect();
        assert_eq!(lines2.len(), 2);
        assert_eq!(lines2[1], "2,Maria");

        teardown_table(db_path, table_name1);
        teardown_table(db_path, table_name2);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_table_with_empty_update_data() {
        let db_path = "./test_update_db5";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );

        let update_data = HashMap::new(); // Sin datos para actualizar
        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &update_data, &where_cond, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "1,Carlos"); // No debe cambiar porque no se proporcionaron datos de actualización

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_with_invalid_column() {
        let db_path = "./test_update_db6";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );

        let mut updates = HashMap::new();
        updates.insert("invalid_column".to_string(), "value".to_string()); // Columna no válida

        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &updates, &where_condition, db_path);

        assert!(
            update_result.is_err(),
            "Expected error when updating with an invalid column"
        );

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_no_matching_rows() {
        let db_path = "./test_update_db7";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&["1".to_string(), "Carlos".to_string()]),
        );

        let mut updates = HashMap::new();
        updates.insert("nombre".to_string(), "Juan".to_string());

        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "2".to_string(),
        )); // No hay id 2

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &updates, &where_condition, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        // Verificar que la tabla siga igual
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2); // 1 fila de datos + 1 fila de headers
        assert_eq!(lines[1], "1,Carlos"); // Debe seguir siendo "Carlos"

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_update_empty_table() {
        let db_path = "./test_update_db8";
        let table_name = "empty_table";

        // Crear una tabla vacía
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            None,
        ); // Sin filas

        let mut updates = HashMap::new();
        updates.insert("nombre".to_string(), "Juan".to_string());

        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let update_result = update(&tables, &updates, &where_condition, db_path);

        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        // Verifica que la tabla siga igual
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "id,nombre"); // Solo debe tener los headers

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
}
