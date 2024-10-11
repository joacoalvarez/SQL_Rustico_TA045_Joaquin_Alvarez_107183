#![allow(clippy::module_name_repetitions)]

#[derive(Debug)]
/// Representacion de las direcciones de ordenamiento
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug)]
/// Estructura que maneja la direccion de cada columna a ordenar
pub struct OrderBy {
    pub column: String,
    pub direction: Direction,
}
