use super::utils_commands::{create_row_values_map, create_table_list, open_csv_reader};
use crate::commands::utils_commands::should_filter;
use crate::condition::Condition;
use crate::errors::ErrorType;
use crate::order::{Direction, OrderBy};
use std::io::BufRead;

/// Verifica que los headers seleccionados estén en la tabla
/// Si los headers contienen un asterisco (*), selecciona todos los headers de la tabla.
fn check_select_headers(
    table_header: &[String],
    headers: &[String],
) -> Result<Vec<String>, ErrorType> {
    if headers.len() == 1 && headers[0] == "*" {
        // Si solo hay un asterisco, devolvemos todos los headers de la tabla
        return Ok(table_header.to_vec());
    }

    // Si no hay asterisco, verificamos que todos los headers existan en la tabla
    for header in headers {
        if !table_header.contains(header) {
            return Err(ErrorType::InvalidColumn(format!(
                "Column to select '{header}' not found in the table"
            )));
        }
    }

    // Retornamos los headers seleccionados si son válidos
    Ok(headers.to_vec())
}

/// Ordena las filas según lo pedido en la query
fn sort_rows(
    selected_rows: &mut [Vec<String>],
    headers: &[String],
    criteras: &[OrderBy],
) -> Result<(), ErrorType> {
    // Verificar si todas las columnas de ordenación existen en los headers
    for OrderBy { column, .. } in criteras {
        if !headers.contains(column) {
            return Err(ErrorType::InvalidColumn(format!(
                "Column '{column}' not found in headers"
            )));
        }
    }

    // ordena si las columnas son válidas
    selected_rows.sort_by(|a, b| {
        for OrderBy { column, direction } in criteras {
            if let Some(idx) = headers.iter().position(|header| header == column) {
                if idx < a.len() && idx < b.len() {
                    let value_a = &a[idx];
                    let value_b = &b[idx];

                    let cmp = value_a.cmp(value_b);
                    if cmp != std::cmp::Ordering::Equal {
                        return match direction {
                            Direction::Ascending => cmp,
                            Direction::Descending => cmp.reverse(),
                        };
                    }
                }
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(())
}

/// funcion auxiliar para retornar el valor del output y utilizar en testings
fn get_select_output(select_headers: &[String], selected_rows: &[Vec<String>]) -> String {
    let mut output = String::new();

    // Agregamos los headers
    output.push_str(&select_headers.join(","));
    output.push('\n'); // Añadimos una nueva línea después de los headers

    // Agregamos cada fila
    for line in selected_rows {
        output.push_str(&line.join(","));
        output.push('\n'); // Añadimos una nueva línea después de cada fila
    }

    output
}

pub fn select(
    headers: &[String],
    tables: &[String],
    where_st: &Option<Condition>,
    order: &Option<Vec<OrderBy>>,
    db_path: &str,
) -> Result<Option<String>, ErrorType> {
    let table_list = create_table_list(tables, db_path)?;
    let mut output = String::new();

    for table in table_list {
        // abro archivo
        let (mut reader, table_header, _) = open_csv_reader(&table, db_path)
            .map_err(|e| ErrorType::InvalidTable(format!("Error opening table: {e}")))?;

        let select_headers = check_select_headers(&table_header, headers)?;

        // filtro el where, dejo los headers selccionados
        let mut selected_rows: Vec<Vec<String>> = Vec::new();
        let mut buffer = String::new();
        while reader
            .read_line(&mut buffer)
            .map_err(|e| ErrorType::OtherError(format!("Error reading line: {e}")))?
            > 0
        {
            let row_values_map = create_row_values_map(&table_header, &buffer);
            buffer.clear();

            let should_select = should_filter(where_st, &row_values_map);

            if should_select {
                let selected_row: Vec<String> = select_headers
                    .iter()
                    .map(|select_headers| row_values_map[select_headers].to_string())
                    .collect();
                selected_rows.push(selected_row);
            }
        }

        // con los seleccionados del where ordeno segun criterio pedido
        if let Some(criteras) = order {
            sort_rows(&mut selected_rows, &select_headers, criteras)?;
        }
        output.push_str(&get_select_output(&select_headers, &selected_rows));
        println!("{}", select_headers.join(","));
        for line in selected_rows {
            println!("{}", line.join(","));
        }
    }
    Ok(Some(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::{ComparisonOp, Condition};
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;

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
        // Elimina el archivo de la tabla después del test
        let csv_path = format!("{}/{}.csv", db_path, table_name);
        if Path::new(&csv_path).exists() {
            fs::remove_file(csv_path).expect("Failed to delete test table file");
        }
    }

    #[test]
    fn test_select_with_where_condition() {
        let db_path = "./test_select_db1";
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
    
        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));
    
        let tables = vec![table_name.to_string()];
        let select_result = select(&headers, &tables, &where_condition, &None, db_path);
    
        // Verificar el output
        let expected_output = "id,nombre\n1,Juan\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_select_where_no_eq() {
        let db_path = "./test_select_db2";
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
    
        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "3".to_string(),
        ));
    
        let tables = vec![table_name.to_string()];
        let select_result = select(&headers, &tables, &where_condition, &None, db_path);

    
        // Como no hay filas que coincidan con la condicion, el es solo los headers
        let expected_output = "id,nombre\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
    
    #[test]
    fn test_select_without_where_condition() {
        let db_path = "./test_select_db3";
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
    
        let tables = vec![table_name.to_string()];
        let select_result = select(&headers, &tables, &None, &None, db_path);
    
        // Verificar el output sin condición WHERE, debería incluir todas las filas
        let expected_output = "id,nombre\n1,Juan\n2,Maria\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
    
    #[test]
    fn test_select_with_order() {
        let db_path = "./test_select_db4";
        let table_name = "test_table";
        let headers = vec!["id".to_string(), "nombre".to_string()];
    
        // Configurar la tabla con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name,
            &headers,
            Some(&[
                &["2".to_string(), "Maria".to_string()],
                &["1".to_string(), "Juan".to_string()],
            ]),
        );
    
        let order_by = vec![OrderBy {
            column: "id".to_string(),
            direction: Direction::Ascending,
        }];
        let tables = vec![table_name.to_string()];
        let select_result = select(&headers, &tables, &None, &Some(order_by), db_path);
    
        // Verificar el output con ORDER BY ascendente
        let expected_output = "id,nombre\n1,Juan\n2,Maria\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
    
    #[test]
    fn test_select_with_where_condition_and_order() {
        let db_path = "./test_select_db6";
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
    
        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "1".to_string(),
        ));
        let order_by = vec![OrderBy {
            column: "nombre".to_string(),
            direction: Direction::Ascending,
        }];
        let tables = vec![table_name.to_string()];
        let select_result = select(
            &headers,
            &tables,
            &where_condition,
            &Some(order_by),
            db_path,
        );
    
        // Verificar el output
        let expected_output = "id,nombre\n1,Juan\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
    
    #[test]
    fn test_select_where_no_eq_rows_with_order() {
        let db_path = "./test_select_db7";
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
    
        let where_condition = Some(Condition::Comparison(
            "id".to_string(),
            ComparisonOp::Eq,
            "3".to_string(),
        ));
        let order_by = vec![OrderBy {
            column: "nombre".to_string(),
            direction: Direction::Ascending,
        }];
        let tables = vec![table_name.to_string()];
        let select_result = select(
            &headers,
            &tables,
            &where_condition,
            &Some(order_by),
            db_path,
        );
    
        // Como no hay filas que coincidan con la condición WHERE, solo deben aparecer los headers
        let expected_output = "id,nombre\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
    
    #[test]
    fn test_select_with_order_descending() {
        let db_path = "./test_select_db8";
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
                &["3".to_string(), "Ana".to_string()],
                &["2".to_string(), "Maria".to_string()],
            ]),
        );
    
        // Ordenamiento descendiente de id
        let order_by = vec![OrderBy {
            column: "id".to_string(),
            direction: Direction::Descending,
        }];
        let tables = vec![table_name.to_string()];
    
        // Ejecutar la selección
        let select_result = select(&headers, &tables, &None, &Some(order_by), db_path);
    
        // Verificar el output
        let expected_output = "id,nombre\n3,Ana\n2,Maria\n1,Juan\n";
        assert_eq!(select_result.unwrap().unwrap(), expected_output);
    
        teardown_table(db_path, table_name);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }

    #[test]
    fn test_select_all_columns_from_multiple_tables() {
        let db_path = "./test_select_db_multiple";
        let table_name1 = "test_table1";
        let table_name2 = "test_table2";
        
        let headers1 = vec!["id".to_string(), "nombre".to_string()];
        let headers2 = vec!["id".to_string(), "producto".to_string()];
        
        // Crear las tablas con datos
        fs::create_dir_all(&db_path).expect("Failed to create test database directory");
        setup_table(
            db_path,
            table_name1,
            &headers1,
            Some(&[
                &["1".to_string(), "Juan".to_string()],
                &["2".to_string(), "Maria".to_string()],
            ]),
        );
        
        setup_table(
            db_path,
            table_name2,
            &headers2,
            Some(&[
                &["1".to_string(), "Laptop".to_string()],
                &["2".to_string(), "Mouse".to_string()],
            ]),
        );

        let tables = vec![table_name1.to_string(), table_name2.to_string()];
        let headers = vec!["*".to_string()]; // SELECT *

        let select_result = select(&headers, &tables, &None, &None, db_path);
        assert!(select_result.is_ok(), "Select failed: {:?}", select_result);

        let output = select_result.unwrap();
        assert!(output.is_some(), "Output is None");

        let expected_output = "id,nombre\n1,Juan\n2,Maria\nid,producto\n1,Laptop\n2,Mouse\n";
        assert_eq!(output.unwrap(), expected_output, "Output did not match expected");

        teardown_table(db_path, table_name1);
        teardown_table(db_path, table_name2);
        fs::remove_dir_all(db_path).expect("Failed to delete test database directory");
    }
}
