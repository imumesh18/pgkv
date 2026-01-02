//! Error types for pgkv operations.

use std::fmt;

/// Result type alias for pgkv operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types that can occur during pgkv operations.
#[derive(Debug)]
pub enum Error {
    /// The requested key was not found in the store.
    NotFound {
        /// The key that was not found.
        key: String,
    },

    /// A database connection error occurred.
    Connection(String),

    /// A database query error occurred.
    Query(String),

    /// The provided key is invalid (empty or too long).
    InvalidKey {
        /// The reason the key is invalid.
        reason: String,
    },

    /// The provided value is invalid (too large).
    InvalidValue {
        /// The reason the value is invalid.
        reason: String,
    },

    /// A compare-and-swap operation failed due to value mismatch.
    CasMismatch {
        /// The key involved in the failed CAS operation.
        key: String,
    },

    /// The key has expired.
    Expired {
        /// The key that has expired.
        key: String,
    },

    /// A transaction error occurred.
    Transaction(String),

    /// The table does not exist and auto-creation is disabled.
    TableNotFound {
        /// The name of the missing table.
        table: String,
    },

    /// Configuration error.
    Config(String),

    /// An I/O error occurred.
    Io(std::io::Error),

    /// Serialization/deserialization error (with serde feature).
    #[cfg(feature = "serde")]
    Serialization(String),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotFound { key } => write!(f, "key not found: {}", key),
            Error::Connection(msg) => write!(f, "connection error: {}", msg),
            Error::Query(msg) => write!(f, "query error: {}", msg),
            Error::InvalidKey { reason } => write!(f, "invalid key: {}", reason),
            Error::InvalidValue { reason } => write!(f, "invalid value: {}", reason),
            Error::CasMismatch { key } => {
                write!(f, "compare-and-swap failed for key: {}", key)
            }
            Error::Expired { key } => write!(f, "key has expired: {}", key),
            Error::Transaction(msg) => write!(f, "transaction error: {}", msg),
            Error::TableNotFound { table } => {
                write!(f, "table not found: {}", table)
            }
            Error::Config(msg) => write!(f, "configuration error: {}", msg),
            Error::Io(e) => write!(f, "I/O error: {}", e),
            #[cfg(feature = "serde")]
            Error::Serialization(msg) => write!(f, "serialization error: {}", msg),
        }
    }
}

impl From<postgres::Error> for Error {
    fn from(err: postgres::Error) -> Self {
        if err.is_closed() {
            Error::Connection(err.to_string())
        } else {
            Error::Query(err.to_string())
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl Error {
    /// Returns `true` if this error indicates the key was not found.
    #[inline]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound { .. })
    }

    /// Returns `true` if this error indicates the key has expired.
    #[inline]
    pub fn is_expired(&self) -> bool {
        matches!(self, Error::Expired { .. })
    }

    /// Returns `true` if this error is a CAS mismatch.
    #[inline]
    pub fn is_cas_mismatch(&self) -> bool {
        matches!(self, Error::CasMismatch { .. })
    }

    /// Returns `true` if this error is a connection error.
    #[inline]
    pub fn is_connection(&self) -> bool {
        matches!(self, Error::Connection(_))
    }

    /// Returns `true` if this error is recoverable (can retry).
    #[inline]
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Error::Connection(_) | Error::CasMismatch { .. } | Error::Expired { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::NotFound {
            key: "test".to_string(),
        };
        assert_eq!(err.to_string(), "key not found: test");

        let err = Error::CasMismatch {
            key: "test".to_string(),
        };
        assert!(err.to_string().contains("compare-and-swap"));
    }

    #[test]
    fn test_error_predicates() {
        let not_found = Error::NotFound {
            key: "test".to_string(),
        };
        assert!(not_found.is_not_found());
        assert!(!not_found.is_expired());
        assert!(!not_found.is_recoverable());

        let cas = Error::CasMismatch {
            key: "test".to_string(),
        };
        assert!(cas.is_cas_mismatch());
        assert!(cas.is_recoverable());

        let conn = Error::Connection("test".to_string());
        assert!(conn.is_connection());
        assert!(conn.is_recoverable());
    }
}
