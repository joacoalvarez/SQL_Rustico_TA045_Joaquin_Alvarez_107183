use std::error::Error;
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
/// Representacion de los posibles tipos de errores
pub enum ErrorType {
    InvalidTable(String),
    InvalidColumn(String),
    InvalidSyntax(String),
    OtherError(String),
}

impl ErrorType {
    /// Dado un `ErrorType` retorna un error con el correcto formato
    #[must_use]
    pub fn create_error(&self) -> Box<dyn Error> {
        let (error_type, message) = match self {
            ErrorType::InvalidTable(msg) => ("INVALID_TABLE", msg),
            ErrorType::InvalidColumn(msg) => ("INVALID_COLUMN", msg),
            ErrorType::InvalidSyntax(msg) => ("INVALID_SYNTAX", msg),
            ErrorType::OtherError(msg) => ("ERROR", msg),
        };

        Box::from(format!("[{error_type}]: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_table_error() {
        let error = ErrorType::InvalidTable("Table not found".to_string());
        let formatted_error = error.create_error();

        assert_eq!(
            formatted_error.to_string(),
            "[INVALID_TABLE]: Table not found"
        );
    }

    #[test]
    fn test_invalid_column_error() {
        let error = ErrorType::InvalidColumn("Column does not exist".to_string());
        let formatted_error = error.create_error();

        assert_eq!(
            formatted_error.to_string(),
            "[INVALID_COLUMN]: Column does not exist"
        );
    }

    #[test]
    fn test_invalid_syntax_error() {
        let error = ErrorType::InvalidSyntax("Syntax error in query".to_string());
        let formatted_error = error.create_error();

        assert_eq!(
            formatted_error.to_string(),
            "[INVALID_SYNTAX]: Syntax error in query"
        );
    }

    #[test]
    fn test_other_error() {
        let error = ErrorType::OtherError("An unknown error occurred".to_string());
        let formatted_error = error.create_error();

        assert_eq!(
            formatted_error.to_string(),
            "[ERROR]: An unknown error occurred"
        );
    }
}
