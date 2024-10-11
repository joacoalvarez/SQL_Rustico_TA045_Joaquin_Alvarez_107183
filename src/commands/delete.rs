use super::utils_commands::{
    create_aux_file, create_row_values_map, create_table_list, open_csv_reader, should_filter,
};
use crate::condition::Condition;
use crate::errors::ErrorType;
use std::{io::BufRead, io::Write};

pub fn delete(
    tables: &[String],
    where_st: &Option<Condition>,
    db_path: &str,
) -> Result<Option<String>, ErrorType> {
    let table_list = create_table_list(tables, db_path)?;

    for table in table_list {
        // abro archivo y auxiliar
        let (mut reader, table_header, _) = open_csv_reader(&table, db_path)
            .map_err(|e| ErrorType::InvalidTable(format!("opening table {e} failed")))?;

        let (mut aux_file, csv_table, aux_table) = create_aux_file(&table, db_path, &table_header)
            .map_err(|e| ErrorType::OtherError(format!("creating auxiliary file {e} failed")))?;

        // evaluo la fila por la condicion where
        let mut buffer = String::new();
        while reader
            .read_line(&mut buffer)
            .map_err(|e| ErrorType::OtherError(format!("Reading line failed {e}")))?
            > 0
        {
            let row_values_map = create_row_values_map(&table_header, &buffer);

            let should_delete = should_filter(where_st, &row_values_map);

            // si no hay que borrar lo escribo en el auxiliar
            if !should_delete {
                let t_buffer = buffer.trim();
                writeln!(aux_file, "{t_buffer}").map_err(|e| {
                    ErrorType::OtherError(format!("writing to auxiliary file {e} failed"))
                })?;
            }
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

    // Función auxiliar para crear la tabla y agregar datos
    fn setup_table(
        db_path: &str,
        table_name: &str,
        headers: &[String],
        data: Option<&[&[String]]>,
    ) {
        let csv_path = format!("{}/{}.csv", db_path, table_name);
        let mut file = File::create(&csv_path).expect("Unable to create test table file");

        // Escribir headers en el archivo CSV
        writeln!(file, "{}", headers.join(",")).expect("Unable to write headers");

        if let Some(rows) = data {
            for row in rows {
                writeln!(file, "{}", row.join(",")).expect("Unable to write row data");
            }
        }
    }

    fn teardown_table(db_path: &str, table_name: &str) {
        let csv_path = format!("{}/{}.csv", db_path, table_name);
        fs::remove_file(csv_path).expect("Failed to delete test table file");
    }

    #[test]
    fn test_delete_single_row() {
        let db_path = "./test_delete_db1";
        let table_name = "test_table";
        let headers = vec!["id".to_string(), "nombre".to_string()];

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &headers,
            Some(&[
                &["1".to_string(), "Juan".to_string()],
                &["2".to_string(), "Maria".to_string()],
            ]),
        );

        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let delete_result = delete(&tables, &where_cond, db_path);

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        // Verificar que solo se haya eliminado la fila correspondiente
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "2,Maria");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_delete_no_rows() {
        let db_path = "./test_delete_db2";
        let table_name = "test_table";
        let headers = vec!["id".to_string(), "nombre".to_string()];

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &headers,
            Some(&[
                &["1".to_string(), "Juan".to_string()],
                &["2".to_string(), "Maria".to_string()],
            ]),
        );

        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "3".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let delete_result = delete(&tables, &where_cond, db_path);

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        // Verificar el contenido de la tabla antes del delete
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(&csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[1], "1,Juan");
        assert_eq!(lines[2], "2,Maria");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_delete_multiple_rows() {
        let db_path = "./test_delete_db3";
        let table_name = "test_table";
        let headers = vec!["id".to_string(), "nombre".to_string()];

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &headers,
            Some(&[
                &["1".to_string(), "Juan".to_string()],
                &["1".to_string(), "Maria".to_string()],
                &["2".to_string(), "Ana".to_string()],
            ]),
        );

        // Condición para eliminar todas las filas donde id es 1
        let where_cond = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));

        let tables = vec![table_name.to_string()];
        let delete_result = delete(&tables, &where_cond, db_path);

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        // Verificar que solo se haya eliminado las filas correspondientes
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1], "2,Ana");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_delete_empty_table() {
        let db_path = "./test_delete_db7";
        let table_name = "empty_table";

        // Crear una tabla vacía
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            None,
        ); // Sin filas

        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        )); // No hay filas

        let tables = vec![table_name.to_string()];
        let delete_result = delete(&tables, &where_condition, db_path);

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        // Verificar que la tabla siga igual
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "id,nombre"); // Solo debe tener los headers

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_delete_no_matching_rows() {
        let db_path = "./test_delete_db8";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&[
                &["1".to_string(), "Juan".to_string()],
                &["2".to_string(), "Maria".to_string()],
            ]),
        );

        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "3".to_string(),
        )); // No hay id 3

        let tables = vec![table_name.to_string()];
        let delete_result = delete(&tables, &where_condition, db_path);

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        // Verificar que la tabla siga igual
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[1], "1,Juan");
        assert_eq!(lines[2], "2,Maria");

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_delete_without_where_condition() {
        let db_path = "./test_delete_db9";
        let table_name = "test_table";

        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &vec!["id".to_string(), "nombre".to_string()],
            Some(&[
                &["1".to_string(), "Juan".to_string()],
                &["2".to_string(), "Maria".to_string()],
            ]),
        );

        // sin condición where
        let tables = vec![table_name.to_string()];
        let delete_result = delete(&tables, &None, db_path); // Sin condición

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        // Verificar que la tabla esté vacía
        let csv_table_path = format!("{}/{}.csv", db_path, table_name);
        let content = fs::read_to_string(csv_table_path).expect("Failed to read test table file");
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "id,nombre"); // Solo debe tener los headers

        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
}
