use sql_rustico::{command_types::Commands, commands::execute, parser::parse_query};
use std::fs::create_dir_all;
use std::{error::Error, fs};

fn sql_main_replica(db_path: &str, query: &str) -> Result<Option<String>, Box<dyn Error>> {
    // Parseo la query, chequeo errores y obtengo el comando
    let command: Commands = parse_query(&mut query.to_string()).map_err(|e| e.create_error())?;

    // Ejecuto el comando y busco errores
    let output = execute(&command, db_path).map_err(|e| e.create_error())?;

    Ok(output)
}
#[test]
fn test_select_ordenes_with_quantity_greater_than_one() -> Result<(), Box<dyn Error>> {
    let db_path = "tablas";

    let output = sql_main_replica(
        db_path,
        "SELECT id, producto, id_cliente FROM ordenes WHERE cantidad > 1;",
    )?;

    let expected_output = Some(
        "id,producto,id_cliente\n\
                                                102,Teléfono,2\n\
                                                105,Mouse,4\n\
                                                110,Teléfono,6\n"
            .to_string(),
    );

    assert_eq!(output, expected_output);
    Ok(())
}

#[test]
fn test_select_ordenes_with_complex_conditions() -> Result<(), Box<dyn Error>> {
    let db_path = "tablas";

    let output = sql_main_replica(db_path, "SELECT id, producto, cantidad FROM ordenes WHERE (cantidad > 1 AND id_cliente = 2) OR (producto = 'Laptop' AND NOT id_cliente = 1) ORDER BY cantidad DESC, producto ASC;")?;

    let expected_output = Some(
        "id,producto,cantidad\n\
                                                102,Teléfono,2\n\
                                                109,Laptop,1\n"
            .to_string(),
    ); // Verifica el orden esperado aquí

    assert_eq!(output, expected_output);
    Ok(())
}

#[test]
fn test_select_clientes_with_last_name_lopez_ordered_by_email_desc() -> Result<(), Box<dyn Error>> {
    let db_path = "tablas";

    let output = sql_main_replica(
        db_path,
        "SELECT id, nombre, email FROM clientes WHERE apellido = 'López' ORDER BY email DESC;",
    )?;

    let expected_output = Some(
        "id,nombre,email\n\
                                                5,José,jose.lopez@email.com\n\
                                                2,Ana,ana.lopez@email.com\n"
            .to_string(),
    );

    assert_eq!(output, expected_output);
    Ok(())
}

#[test]
fn test_update_cliente_email() -> Result<(), Box<dyn Error>> {
    let test_dir = "tests/temp_db1";

    // Crea el directorio temporal para pruebas
    create_dir_all(test_dir)?;

    // Copia los archivos originales a la carpeta de pruebas
    fs::copy("tablas/clientes.csv", format!("{}/clientes.csv", test_dir))?;

    sql_main_replica(
        test_dir,
        "UPDATE clientes SET email = 'mrodriguez@hotmail.com' WHERE id = 4;",
    )?;

    let updated_content = fs::read_to_string(format!("{}/clientes.csv", test_dir))?;
    let expected_content = "id,nombre,apellido,email\n\
                                1,Juan,Pérez,juan.perez@email.com\n\
                                2,Ana,López,ana.lopez@email.com\n\
                                3,Carlos,Gómez,carlos.gomez@email.com\n\
                                4,María,Rodríguez,mrodriguez@hotmail.com\n\
                                5,José,López,jose.lopez@email.com\n\
                                6,Laura,Fernández,laura.fernandez@email.com\n";

    assert_eq!(updated_content, expected_content);

    // Limpia el directorio temporal
    fs::remove_dir_all(test_dir)?;

    Ok(())
}

#[test]
fn test_insert_ordenes() -> Result<(), Box<dyn Error>> {
    let test_dir = "tests/temp_db2";

    // Crea el directorio temporal para pruebas
    create_dir_all(test_dir)?;

    // Copia los archivos originales a la carpeta de pruebas
    fs::copy("tablas/ordenes.csv", format!("{}/ordenes.csv", test_dir))?;

    sql_main_replica(
        test_dir,
        "INSERT INTO ordenes (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3);",
    )?;

    let updated_orders_content = fs::read_to_string(format!("{}/ordenes.csv", test_dir))?;
    let expected_orders_content = "id,id_cliente,producto,cantidad\n\
                                        101,1,Laptop,1\n\
                                        103,1,Monitor,1\n\
                                        102,2,Teléfono,2\n\
                                        104,3,Teclado,1\n\
                                        105,4,Mouse,2\n\
                                        106,5,Impresora,1\n\
                                        107,6,Altavoces,1\n\
                                        108,4,Auriculares,1\n\
                                        109,5,Laptop,1\n\
                                        110,6,Teléfono,2\n\
                                        111,6,Laptop,3\n";

    assert_eq!(updated_orders_content, expected_orders_content);

    // Limpia el directorio temporal
    fs::remove_dir_all(test_dir)?;

    Ok(())
}

#[test]
fn test_delete_cliente() -> Result<(), Box<dyn Error>> {
    let test_dir = "tests/temp_db3";

    // Crea el directorio temporal para pruebas
    create_dir_all(test_dir)?;

    // Copia los archivos originales a la carpeta de pruebas
    fs::copy("tablas/clientes.csv", format!("{}/clientes.csv", test_dir))?;

    sql_main_replica(test_dir, "DELETE FROM clientes WHERE id = 4;")?;

    let updated_content = fs::read_to_string(format!("{}/clientes.csv", test_dir))?;
    let expected_content = "id,nombre,apellido,email\n\
                                1,Juan,Pérez,juan.perez@email.com\n\
                                2,Ana,López,ana.lopez@email.com\n\
                                3,Carlos,Gómez,carlos.gomez@email.com\n\
                                5,José,López,jose.lopez@email.com\n\
                                6,Laura,Fernández,laura.fernandez@email.com\n";

    assert_eq!(updated_content, expected_content);

    // Limpia el directorio temporal
    fs::remove_dir_all(test_dir)?;

    Ok(())
}
