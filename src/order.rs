#[derive(Debug)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Debug)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDirection,
}
