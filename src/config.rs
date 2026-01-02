//! Configuration types for pgkv.

use crate::{Error, Result};

/// The type of table to use for storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableType {
    /// UNLOGGED table - faster writes but not crash-safe.
    ///
    /// Data will be lost after an unclean shutdown.
    /// Ideal for caching, sessions, and ephemeral data.
    #[default]
    Unlogged,

    /// Regular table with full WAL support.
    ///
    /// Slower writes but crash-safe.
    /// Use for data that must survive restarts.
    Regular,
}

impl TableType {
    /// Returns the SQL keyword for this table type.
    #[inline]
    pub fn sql_keyword(&self) -> &'static str {
        match self {
            TableType::Unlogged => "UNLOGGED",
            TableType::Regular => "",
        }
    }
}

/// Strategy for handling expired keys.
///
/// This controls how and when expired keys are cleaned up from the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TtlCleanupStrategy {
    /// Automatically delete expired keys when they are accessed (lazy cleanup).
    ///
    /// This is the default behavior. Expired keys are deleted during read operations
    /// (get, get_many, etc.) when encountered. This provides automatic cleanup without
    /// requiring a background task, but may leave expired keys in the table until accessed.
    #[default]
    OnRead,

    /// Never automatically delete expired keys - user must call `cleanup_expired()` manually.
    ///
    /// Use this when you want full control over when cleanup happens, or when you want
    /// to run cleanup on a schedule (e.g., via cron or a background task).
    ///
    /// Expired keys will still be excluded from read results, but they will remain
    /// in the table until explicitly cleaned up.
    Manual,

    /// Disable TTL expiration checking entirely.
    ///
    /// Expired keys will be treated as valid and returned in results.
    /// Use this only if you don't use TTL features and want maximum read performance.
    Disabled,
}

/// Configuration options for the key-value store.
///
/// # Example
///
/// ```rust,no_run
/// use pgkv::{Config, TableType, TtlCleanupStrategy};
///
/// let config = Config::new("postgresql://localhost/mydb")
///     .table_name("my_cache")
///     .table_type(TableType::Unlogged)
///     .auto_create_table(true)
///     .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead)
///     .max_key_length(1024)
///     .max_value_size(10 * 1024 * 1024); // 10MB
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL connection string.
    pub(crate) connection_string: String,

    /// Name of the table to use for storage.
    pub(crate) table_name: String,

    /// Type of table to create.
    pub(crate) table_type: TableType,

    /// Whether to automatically create the table if it doesn't exist.
    pub(crate) auto_create_table: bool,

    /// Strategy for cleaning up expired keys.
    pub(crate) ttl_cleanup_strategy: TtlCleanupStrategy,

    /// Maximum allowed key length in bytes.
    pub(crate) max_key_length: usize,

    /// Maximum allowed value size in bytes.
    pub(crate) max_value_size: usize,

    /// Schema to use for the table.
    pub(crate) schema: Option<String>,

    /// Connection timeout in seconds.
    pub(crate) connect_timeout_secs: u64,

    /// Application name for PostgreSQL connection.
    pub(crate) application_name: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            table_name: "kv_store".to_string(),
            table_type: TableType::Unlogged,
            auto_create_table: true,
            ttl_cleanup_strategy: TtlCleanupStrategy::OnRead,
            max_key_length: 1024,              // 1KB max key
            max_value_size: 100 * 1024 * 1024, // 100MB max value
            schema: None,
            connect_timeout_secs: 10,
            application_name: None,
        }
    }
}

impl Config {
    /// Creates a new configuration with the given connection string.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Config;
    ///
    /// let config = Config::new("postgresql://user:pass@localhost:5432/mydb");
    /// ```
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            ..Default::default()
        }
    }

    /// Sets the table name.
    ///
    /// Default: `"kv_store"`
    pub fn table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = name.into();
        self
    }

    /// Sets the table type (Unlogged or Regular).
    ///
    /// Default: [`TableType::Unlogged`]
    pub fn table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    /// Sets whether to automatically create the table.
    ///
    /// Default: `true`
    pub fn auto_create_table(mut self, auto_create: bool) -> Self {
        self.auto_create_table = auto_create;
        self
    }

    /// Sets the strategy for cleaning up expired keys.
    ///
    /// - [`TtlCleanupStrategy::OnRead`] (default): Automatically delete expired keys when accessed
    /// - [`TtlCleanupStrategy::Manual`]: User must call `cleanup_expired()` manually
    /// - [`TtlCleanupStrategy::Disabled`]: No expiration checking (keys never expire)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::{Config, TtlCleanupStrategy};
    ///
    /// // Manual cleanup - user controls when expired keys are deleted
    /// let config = Config::new("postgresql://localhost/mydb")
    ///     .ttl_cleanup_strategy(TtlCleanupStrategy::Manual);
    /// ```
    pub fn ttl_cleanup_strategy(mut self, strategy: TtlCleanupStrategy) -> Self {
        self.ttl_cleanup_strategy = strategy;
        self
    }

    /// Sets the maximum key length in bytes.
    ///
    /// Default: `1024` (1KB)
    pub fn max_key_length(mut self, length: usize) -> Self {
        self.max_key_length = length;
        self
    }

    /// Sets the maximum value size in bytes.
    ///
    /// Default: `104857600` (100MB)
    pub fn max_value_size(mut self, size: usize) -> Self {
        self.max_value_size = size;
        self
    }

    /// Sets the schema to use for the table.
    ///
    /// Default: `None` (uses default schema, typically `public`)
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Sets the connection timeout in seconds.
    ///
    /// Default: `10`
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    /// Sets the application name for the PostgreSQL connection.
    ///
    /// This appears in `pg_stat_activity` and can help with monitoring.
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// Returns the fully qualified table name (with schema if set).
    pub(crate) fn qualified_table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&self.table_name)),
            None => quote_ident(&self.table_name),
        }
    }

    /// Returns whether TTL expiration checking is enabled.
    #[inline]
    pub(crate) fn ttl_enabled(&self) -> bool {
        self.ttl_cleanup_strategy != TtlCleanupStrategy::Disabled
    }

    /// Returns whether automatic cleanup on read is enabled.
    #[inline]
    pub(crate) fn cleanup_on_read(&self) -> bool {
        self.ttl_cleanup_strategy == TtlCleanupStrategy::OnRead
    }

    /// Validates the configuration.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.connection_string.is_empty() {
            return Err(Error::Config("connection string cannot be empty".into()));
        }

        if self.table_name.is_empty() {
            return Err(Error::Config("table name cannot be empty".into()));
        }

        if self.table_name.len() > 63 {
            return Err(Error::Config(
                "table name exceeds PostgreSQL's 63 character limit".into(),
            ));
        }

        if self.max_key_length == 0 {
            return Err(Error::Config(
                "max_key_length must be greater than 0".into(),
            ));
        }

        if self.max_value_size == 0 {
            return Err(Error::Config(
                "max_value_size must be greater than 0".into(),
            ));
        }

        Ok(())
    }
}

/// Quotes an identifier for safe use in SQL.
fn quote_ident(ident: &str) -> String {
    // PostgreSQL identifier quoting: double any existing quotes
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::new("postgresql://localhost/test");
        assert_eq!(config.table_name, "kv_store");
        assert_eq!(config.table_type, TableType::Unlogged);
        assert!(config.auto_create_table);
        assert_eq!(config.ttl_cleanup_strategy, TtlCleanupStrategy::OnRead);
        assert!(config.ttl_enabled());
        assert!(config.cleanup_on_read());
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new("postgresql://localhost/test")
            .table_name("custom_table")
            .table_type(TableType::Regular)
            .auto_create_table(false)
            .ttl_cleanup_strategy(TtlCleanupStrategy::Manual)
            .max_key_length(2048)
            .max_value_size(1024)
            .schema("custom_schema")
            .application_name("my_app");

        assert_eq!(config.table_name, "custom_table");
        assert_eq!(config.table_type, TableType::Regular);
        assert!(!config.auto_create_table);
        assert_eq!(config.ttl_cleanup_strategy, TtlCleanupStrategy::Manual);
        assert!(config.ttl_enabled());
        assert!(!config.cleanup_on_read());
        assert_eq!(config.max_key_length, 2048);
        assert_eq!(config.max_value_size, 1024);
        assert_eq!(config.schema, Some("custom_schema".to_string()));
        assert_eq!(config.application_name, Some("my_app".to_string()));
    }

    #[test]
    fn test_ttl_cleanup_strategies() {
        let config = Config::new("postgresql://localhost/test")
            .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead);
        assert!(config.ttl_enabled());
        assert!(config.cleanup_on_read());

        let config = Config::new("postgresql://localhost/test")
            .ttl_cleanup_strategy(TtlCleanupStrategy::Manual);
        assert!(config.ttl_enabled());
        assert!(!config.cleanup_on_read());

        let config = Config::new("postgresql://localhost/test")
            .ttl_cleanup_strategy(TtlCleanupStrategy::Disabled);
        assert!(!config.ttl_enabled());
        assert!(!config.cleanup_on_read());
    }

    #[test]
    fn test_qualified_table_name() {
        let config = Config::new("postgresql://localhost/test").table_name("my_table");
        assert_eq!(config.qualified_table_name(), "\"my_table\"");

        let config = Config::new("postgresql://localhost/test")
            .table_name("my_table")
            .schema("my_schema");
        assert_eq!(config.qualified_table_name(), "\"my_schema\".\"my_table\"");
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn test_table_type_sql() {
        assert_eq!(TableType::Unlogged.sql_keyword(), "UNLOGGED");
        assert_eq!(TableType::Regular.sql_keyword(), "");
    }

    #[test]
    fn test_validation() {
        let config = Config::new("");
        assert!(config.validate().is_err());

        let config = Config::new("postgresql://localhost/test").table_name("");
        assert!(config.validate().is_err());

        let config = Config::new("postgresql://localhost/test");
        assert!(config.validate().is_ok());
    }
}
