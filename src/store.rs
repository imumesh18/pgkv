//! The main Store implementation.

use postgres::{Client, NoTls, Row};
use std::cell::RefCell;
use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::types::{CasResult, Entry, KeyValue, ScanOptions, Stats};

/// The main key-value store backed by PostgreSQL.
///
/// Uses interior mutability to provide a clean API with `&self` methods
/// while still allowing database operations.
///
/// # Thread Safety
///
/// `Store` is `!Sync` due to the use of `RefCell`. For multi-threaded access,
/// use a connection pool (like r2d2 or deadpool) with separate `Store` instances
/// per thread, or wrap in `Mutex<Store>`.
///
/// # Example
///
/// ```rust,no_run
/// use pgkv::Store;
///
/// let store = Store::connect("postgresql://localhost/mydb")?;
///
/// // Basic CRUD operations
/// store.set("user:1:name", b"Alice")?;
/// let name = store.get("user:1:name")?;
/// store.delete("user:1:name")?;
/// # Ok::<(), pgkv::Error>(())
/// ```
pub struct Store {
    client: RefCell<Client>,
    config: Config,
    qualified_table: String,
}

impl Store {
    /// Connects to PostgreSQL and creates a new store with default configuration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://user:pass@localhost/mydb")?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn connect(connection_string: &str) -> Result<Self> {
        let config = Config::new(connection_string);
        Self::with_config(config)
    }

    /// Creates a new store with custom configuration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::{Store, Config, TableType};
    ///
    /// let config = Config::new("postgresql://localhost/mydb")
    ///     .table_name("my_cache")
    ///     .table_type(TableType::Unlogged);
    ///
    /// let store = Store::with_config(config)?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn with_config(config: Config) -> Result<Self> {
        config.validate()?;

        let mut client = Client::connect(&config.connection_string, NoTls)
            .map_err(|e| Error::Connection(e.to_string()))?;

        let qualified_table = config.qualified_table_name();

        if config.auto_create_table {
            Self::create_table_internal(&mut client, &config, &qualified_table)?;
        }

        Ok(Self {
            client: RefCell::new(client),
            config,
            qualified_table,
        })
    }

    /// Creates the table and indexes if they don't exist.
    fn create_table_internal(client: &mut Client, config: &Config, table_name: &str) -> Result<()> {
        let table_type = config.table_type.sql_keyword();

        let create_table = format!(
            r#"
            CREATE {table_type} TABLE IF NOT EXISTS {table_name} (
                key TEXT PRIMARY KEY,
                value BYTEA NOT NULL,
                expires_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        );

        client.execute(&create_table, &[])?;

        let idx_name = format!("{}_expires_idx", config.table_name);
        let create_idx = format!(
            r#"CREATE INDEX IF NOT EXISTS "{}" ON {} (expires_at) WHERE expires_at IS NOT NULL"#,
            idx_name, table_name
        );
        client.execute(&create_idx, &[])?;

        Ok(())
    }

    /// Recreates the table (drops all data).
    ///
    /// **Warning**: This will delete all data in the table!
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.recreate_table()?;  // All data is lost!
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn recreate_table(&self) -> Result<()> {
        let mut client = self.client.borrow_mut();
        let drop_sql = format!("DROP TABLE IF EXISTS {}", self.qualified_table);
        client.execute(&drop_sql, &[])?;
        Self::create_table_internal(&mut client, &self.config, &self.qualified_table)?;
        Ok(())
    }

    // ==================== Basic Operations ====================

    /// Gets a value by key.
    ///
    /// Returns `None` if the key doesn't exist or has expired.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// if let Some(value) = store.get("key")? {
    ///     println!("Value: {:?}", value);
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;

        let sql = format!(
            r#"
            SELECT value, expires_at FROM {}
            WHERE key = $1
            "#,
            self.qualified_table
        );

        let row = self.client.borrow_mut().query_opt(&sql, &[&key])?;

        match row {
            Some(row) => {
                // Check expiration if TTL is enabled
                if self.config.ttl_enabled() {
                    if let Some(expires_at) = row.get::<_, Option<SystemTime>>("expires_at") {
                        if expires_at < SystemTime::now() {
                            // Key is expired
                            if self.config.cleanup_on_read() {
                                // Best effort cleanup - ignore errors
                                let _ = self.delete_internal(key);
                            }
                            return Ok(None);
                        }
                    }
                }
                Ok(Some(row.get("value")))
            }
            None => Ok(None),
        }
    }

    /// Gets a value by key, returning an error if not found.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let value = store.get_or_err("key")?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get_or_err(&self, key: &str) -> Result<Vec<u8>> {
        self.get(key)?.ok_or_else(|| Error::NotFound {
            key: key.to_string(),
        })
    }

    /// Gets a value as a UTF-8 string.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// if let Some(s) = store.get_string("key")? {
    ///     println!("String value: {}", s);
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get_string(&self, key: &str) -> Result<Option<String>> {
        self.get(key)?
            .map(|v| {
                String::from_utf8(v).map_err(|e| Error::InvalidValue {
                    reason: format!("value is not valid UTF-8: {}", e),
                })
            })
            .transpose()
    }

    /// Gets the full entry with metadata.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// if let Some(entry) = store.get_entry("key")? {
    ///     println!("Created: {:?}", entry.created_at);
    ///     println!("TTL: {:?}", entry.ttl());
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get_entry(&self, key: &str) -> Result<Option<Entry>> {
        self.validate_key(key)?;

        let sql = format!(
            r#"
            SELECT key, value, expires_at, created_at, updated_at
            FROM {} WHERE key = $1
            "#,
            self.qualified_table
        );

        let row = self.client.borrow_mut().query_opt(&sql, &[&key])?;

        match row {
            Some(row) => {
                let entry = Self::row_to_entry(&row)?;
                if self.config.ttl_enabled() && entry.is_expired() {
                    if self.config.cleanup_on_read() {
                        let _ = self.delete_internal(key);
                    }
                    return Ok(None);
                }
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Sets a value for a key.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.set("key", b"value")?;
    /// store.set("key", "string value")?;  // Also accepts &str
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn set(&self, key: &str, value: impl AsRef<[u8]>) -> Result<()> {
        self.set_internal(key, value.as_ref(), None)
    }

    /// Sets a value with an expiration time (TTL).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    /// use std::time::Duration;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.set_ex("session", b"data", Duration::from_secs(3600))?;  // Expires in 1 hour
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn set_ex(&self, key: &str, value: impl AsRef<[u8]>, ttl: Duration) -> Result<()> {
        let expires_at = SystemTime::now() + ttl;
        self.set_internal(key, value.as_ref(), Some(expires_at))
    }

    /// Sets a value with an absolute expiration time.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    /// use std::time::{SystemTime, Duration};
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let expires_at = SystemTime::now() + Duration::from_secs(3600);
    /// store.set_at("session", b"data", expires_at)?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn set_at(&self, key: &str, value: impl AsRef<[u8]>, expires_at: SystemTime) -> Result<()> {
        self.set_internal(key, value.as_ref(), Some(expires_at))
    }

    fn set_internal(&self, key: &str, value: &[u8], expires_at: Option<SystemTime>) -> Result<()> {
        self.validate_key(key)?;
        self.validate_value(value)?;

        let sql = format!(
            r#"
            INSERT INTO {} (key, value, expires_at, created_at, updated_at)
            VALUES ($1, $2, $3, NOW(), NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                expires_at = EXCLUDED.expires_at,
                updated_at = NOW()
            "#,
            self.qualified_table
        );

        self.client
            .borrow_mut()
            .execute(&sql, &[&key, &value, &expires_at])?;
        Ok(())
    }

    /// Sets a value only if the key doesn't exist.
    ///
    /// Returns `true` if the value was set, `false` if the key already exists.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// if store.set_nx("key", b"value")? {
    ///     println!("Key was set");
    /// } else {
    ///     println!("Key already exists");
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn set_nx(&self, key: &str, value: impl AsRef<[u8]>) -> Result<bool> {
        self.set_nx_internal(key, value.as_ref(), None)
    }

    /// Sets a value with TTL only if the key doesn't exist.
    pub fn set_nx_ex(&self, key: &str, value: impl AsRef<[u8]>, ttl: Duration) -> Result<bool> {
        let expires_at = SystemTime::now() + ttl;
        self.set_nx_internal(key, value.as_ref(), Some(expires_at))
    }

    fn set_nx_internal(
        &self,
        key: &str,
        value: &[u8],
        expires_at: Option<SystemTime>,
    ) -> Result<bool> {
        self.validate_key(key)?;
        self.validate_value(value)?;

        let sql = format!(
            r#"
            INSERT INTO {} (key, value, expires_at, created_at, updated_at)
            VALUES ($1, $2, $3, NOW(), NOW())
            ON CONFLICT (key) DO NOTHING
            "#,
            self.qualified_table
        );

        let count = self
            .client
            .borrow_mut()
            .execute(&sql, &[&key, &value, &expires_at])?;
        Ok(count > 0)
    }

    /// Deletes a key.
    ///
    /// Returns `true` if the key was deleted, `false` if it didn't exist.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.delete("key")?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn delete(&self, key: &str) -> Result<bool> {
        self.validate_key(key)?;
        self.delete_internal(key)
    }

    fn delete_internal(&self, key: &str) -> Result<bool> {
        let sql = format!("DELETE FROM {} WHERE key = $1", self.qualified_table);
        let count = self.client.borrow_mut().execute(&sql, &[&key])?;
        Ok(count > 0)
    }

    /// Checks if a key exists (and is not expired).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// if store.exists("key")? {
    ///     println!("Key exists");
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn exists(&self, key: &str) -> Result<bool> {
        self.validate_key(key)?;

        let sql = format!(
            r#"
            SELECT 1 FROM {} WHERE key = $1
            AND (expires_at IS NULL OR expires_at > NOW())
            "#,
            self.qualified_table
        );

        let row = self.client.borrow_mut().query_opt(&sql, &[&key])?;
        Ok(row.is_some())
    }

    // ==================== Batch Operations ====================

    /// Gets multiple values by keys.
    ///
    /// Returns a vector of key-value pairs for keys that exist and haven't expired.
    /// Order is not guaranteed to match input order.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let results = store.get_many(&["key1", "key2", "key3"])?;
    /// for kv in results {
    ///     println!("{}: {:?}", kv.key, kv.value);
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get_many(&self, keys: &[&str]) -> Result<Vec<KeyValue>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        for key in keys {
            self.validate_key(key)?;
        }

        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();

        let sql = format!(
            r#"
            SELECT key, value FROM {}
            WHERE key = ANY($1)
            AND (expires_at IS NULL OR expires_at > NOW())
            "#,
            self.qualified_table
        );

        let rows = self.client.borrow_mut().query(&sql, &[&keys])?;

        Ok(rows
            .into_iter()
            .map(|row| KeyValue {
                key: row.get("key"),
                value: row.get("value"),
            })
            .collect())
    }

    /// Sets multiple key-value pairs atomically.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.set_many(&[
    ///     ("key1", b"value1".as_slice()),
    ///     ("key2", b"value2"),
    ///     ("key3", b"value3"),
    /// ])?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn set_many(&self, items: &[(&str, &[u8])]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        for (key, value) in items {
            self.validate_key(key)?;
            self.validate_value(value)?;
        }

        let sql = format!(
            r#"
            INSERT INTO {} (key, value, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW()
            "#,
            self.qualified_table
        );

        let mut client = self.client.borrow_mut();

        // Execute in a transaction
        client.execute("BEGIN", &[])?;

        for (key, value) in items {
            if let Err(e) = client.execute(&sql, &[key, value]) {
                let _ = client.execute("ROLLBACK", &[]);
                return Err(e.into());
            }
        }

        client.execute("COMMIT", &[])?;
        Ok(())
    }

    /// Deletes multiple keys.
    ///
    /// Returns the number of keys that were deleted.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let deleted = store.delete_many(&["key1", "key2", "key3"])?;
    /// println!("Deleted {} keys", deleted);
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn delete_many(&self, keys: &[&str]) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }

        for key in keys {
            self.validate_key(key)?;
        }

        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();

        let sql = format!("DELETE FROM {} WHERE key = ANY($1)", self.qualified_table);
        let count = self.client.borrow_mut().execute(&sql, &[&keys])?;
        Ok(count)
    }

    // ==================== Atomic Operations ====================

    /// Atomically increments a numeric value.
    ///
    /// The value is stored as a string representation of an integer.
    /// If the key doesn't exist, it's created with the delta value.
    /// Returns the new value.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let new_value = store.increment("counter", 1)?;
    /// println!("Counter is now: {}", new_value);
    ///
    /// let new_value = store.increment("counter", -1)?;  // Decrement
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn increment(&self, key: &str, delta: i64) -> Result<i64> {
        self.validate_key(key)?;

        // Use a CTE for atomic increment
        let sql = format!(
            r#"
            INSERT INTO {} (key, value, created_at, updated_at)
            VALUES ($1, $2::text::bytea, NOW(), NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = (COALESCE(
                    (encode({}.value, 'escape')::bigint),
                    0
                ) + $3)::text::bytea,
                updated_at = NOW()
            RETURNING encode(value, 'escape')::bigint as new_value
            "#,
            self.qualified_table, self.qualified_table
        );

        let row = self
            .client
            .borrow_mut()
            .query_one(&sql, &[&key, &delta.to_string(), &delta])?;
        Ok(row.get("new_value"))
    }

    /// Atomically decrements a numeric value.
    ///
    /// Equivalent to `increment(key, -delta)`.
    pub fn decrement(&self, key: &str, delta: i64) -> Result<i64> {
        self.increment(key, -delta)
    }

    /// Compare-and-swap operation.
    ///
    /// Atomically sets the value only if the current value matches the expected value.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::{Store, CasResult};
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    ///
    /// // Try to set only if key doesn't exist
    /// match store.compare_and_swap("key", None, b"value")? {
    ///     CasResult::Success => println!("Set successfully"),
    ///     CasResult::Mismatch { current } => println!("Key already exists"),
    ///     CasResult::NotFound => unreachable!(),
    /// }
    ///
    /// // Try to update existing value
    /// match store.compare_and_swap("key", Some(b"value"), b"new_value")? {
    ///     CasResult::Success => println!("Updated successfully"),
    ///     CasResult::Mismatch { current } => println!("Value changed"),
    ///     CasResult::NotFound => println!("Key was deleted"),
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<CasResult> {
        self.validate_key(key)?;
        self.validate_value(new_value)?;

        match expected {
            None => {
                // Expect key to not exist
                if self.set_nx(key, new_value)? {
                    Ok(CasResult::Success)
                } else {
                    let current = self.get(key)?;
                    Ok(CasResult::Mismatch { current })
                }
            }
            Some(expected_value) => {
                // Expect specific value
                let sql = format!(
                    r#"
                    UPDATE {} SET value = $2, updated_at = NOW()
                    WHERE key = $1 AND value = $3
                    AND (expires_at IS NULL OR expires_at > NOW())
                    "#,
                    self.qualified_table
                );

                let count = self
                    .client
                    .borrow_mut()
                    .execute(&sql, &[&key, &new_value, &expected_value])?;

                if count > 0 {
                    Ok(CasResult::Success)
                } else {
                    // Check if key exists to distinguish NotFound from Mismatch
                    match self.get(key)? {
                        Some(current) => Ok(CasResult::Mismatch {
                            current: Some(current),
                        }),
                        None => Ok(CasResult::NotFound),
                    }
                }
            }
        }
    }

    /// Gets the current value and sets a new value atomically.
    ///
    /// Returns the previous value, or `None` if the key didn't exist.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let previous = store.get_and_set("key", b"new_value")?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get_and_set(&self, key: &str, value: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;
        let value = value.as_ref();
        self.validate_value(value)?;

        let sql = format!(
            r#"
            INSERT INTO {} (key, value, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW()
            RETURNING (
                SELECT value FROM {} WHERE key = $1
            ) as old_value
            "#,
            self.qualified_table, self.qualified_table
        );

        let row = self.client.borrow_mut().query_one(&sql, &[&key, &value])?;
        Ok(row.get("old_value"))
    }

    /// Gets the current value and deletes the key atomically.
    ///
    /// Returns the previous value, or `None` if the key didn't exist.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let value = store.get_and_delete("key")?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn get_and_delete(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;

        let sql = format!(
            "DELETE FROM {} WHERE key = $1 RETURNING value",
            self.qualified_table
        );

        let row = self.client.borrow_mut().query_opt(&sql, &[&key])?;
        Ok(row.map(|r| r.get("value")))
    }

    // ==================== TTL Operations ====================

    /// Updates the TTL of an existing key.
    ///
    /// Returns `false` if the key doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    /// use std::time::Duration;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.set("key", b"value")?;
    /// store.expire("key", Duration::from_secs(60))?;  // Expires in 60 seconds
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn expire(&self, key: &str, ttl: Duration) -> Result<bool> {
        self.validate_key(key)?;

        let expires_at = SystemTime::now() + ttl;

        let sql = format!(
            "UPDATE {} SET expires_at = $2, updated_at = NOW() WHERE key = $1",
            self.qualified_table
        );

        let count = self
            .client
            .borrow_mut()
            .execute(&sql, &[&key, &expires_at])?;
        Ok(count > 0)
    }

    /// Removes the TTL from a key (makes it persistent).
    ///
    /// Returns `false` if the key doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.persist("key")?;  // Remove expiration
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn persist(&self, key: &str) -> Result<bool> {
        self.validate_key(key)?;

        let sql = format!(
            "UPDATE {} SET expires_at = NULL, updated_at = NOW() WHERE key = $1",
            self.qualified_table
        );

        let count = self.client.borrow_mut().execute(&sql, &[&key])?;
        Ok(count > 0)
    }

    /// Gets the remaining TTL of a key.
    ///
    /// Returns `None` if the key doesn't exist or has no expiration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// if let Some(ttl) = store.ttl("key")? {
    ///     println!("Key expires in {:?}", ttl);
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn ttl(&self, key: &str) -> Result<Option<Duration>> {
        self.validate_key(key)?;

        let sql = format!(
            "SELECT expires_at FROM {} WHERE key = $1",
            self.qualified_table
        );

        let row = self.client.borrow_mut().query_opt(&sql, &[&key])?;

        match row {
            Some(row) => {
                let expires_at: Option<SystemTime> = row.get("expires_at");
                Ok(expires_at.and_then(|exp| exp.duration_since(SystemTime::now()).ok()))
            }
            None => Ok(None),
        }
    }

    // ==================== Scanning Operations ====================

    /// Lists all keys with optional filtering.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::{Store, ScanOptions};
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    ///
    /// // Get all keys
    /// let keys = store.keys(ScanOptions::new())?;
    ///
    /// // Get keys with prefix
    /// let user_keys = store.keys(ScanOptions::new().prefix("user:"))?;
    ///
    /// // Paginate
    /// let page = store.keys(ScanOptions::new().limit(100).offset(200))?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn keys(&self, options: ScanOptions) -> Result<Vec<String>> {
        let mut sql = format!("SELECT key FROM {} WHERE 1=1", self.qualified_table);

        let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> = vec![];
        let mut param_idx = 1;

        if !options.include_expired {
            sql.push_str(" AND (expires_at IS NULL OR expires_at > NOW())");
        }

        if let Some(ref prefix) = options.prefix {
            sql.push_str(&format!(" AND key LIKE ${}", param_idx));
            params.push(Box::new(format!("{}%", escape_like(prefix))));
            param_idx += 1;
        }

        sql.push_str(" ORDER BY key");

        if let Some(limit) = options.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = options.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let _ = param_idx; // Suppress unused warning

        let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = self.client.borrow_mut().query(&sql, &param_refs)?;
        Ok(rows.into_iter().map(|r| r.get("key")).collect())
    }

    /// Scans key-value pairs with optional filtering.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::{Store, ScanOptions};
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let items = store.scan(ScanOptions::new().prefix("user:").limit(100))?;
    /// for kv in items {
    ///     println!("{}: {:?}", kv.key, kv.value);
    /// }
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn scan(&self, options: ScanOptions) -> Result<Vec<KeyValue>> {
        let mut sql = format!("SELECT key, value FROM {} WHERE 1=1", self.qualified_table);

        let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> = vec![];
        let mut param_idx = 1;

        if !options.include_expired {
            sql.push_str(" AND (expires_at IS NULL OR expires_at > NOW())");
        }

        if let Some(ref prefix) = options.prefix {
            sql.push_str(&format!(" AND key LIKE ${}", param_idx));
            params.push(Box::new(format!("{}%", escape_like(prefix))));
            param_idx += 1;
        }

        sql.push_str(" ORDER BY key");

        if let Some(limit) = options.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = options.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let _ = param_idx; // Suppress unused warning

        let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = self.client.borrow_mut().query(&sql, &param_refs)?;
        Ok(rows
            .into_iter()
            .map(|r| KeyValue {
                key: r.get("key"),
                value: r.get("value"),
            })
            .collect())
    }

    /// Counts keys matching the given options.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::{Store, ScanOptions};
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let count = store.count(ScanOptions::new().prefix("user:"))?;
    /// println!("User keys: {}", count);
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn count(&self, options: ScanOptions) -> Result<u64> {
        let mut sql = format!(
            "SELECT COUNT(*) as count FROM {} WHERE 1=1",
            self.qualified_table
        );

        let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> = vec![];
        let mut param_idx = 1;

        if !options.include_expired {
            sql.push_str(" AND (expires_at IS NULL OR expires_at > NOW())");
        }

        if let Some(ref prefix) = options.prefix {
            sql.push_str(&format!(" AND key LIKE ${}", param_idx));
            params.push(Box::new(format!("{}%", escape_like(prefix))));
            param_idx += 1;
        }

        let _ = param_idx; // Suppress unused warning

        let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref()).collect();

        let row = self.client.borrow_mut().query_one(&sql, &param_refs)?;
        let count: i64 = row.get("count");
        Ok(count as u64)
    }

    /// Deletes all keys matching the prefix.
    ///
    /// Returns the number of keys deleted.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let deleted = store.delete_prefix("temp:")?;
    /// println!("Deleted {} temp keys", deleted);
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn delete_prefix(&self, prefix: &str) -> Result<u64> {
        let sql = format!("DELETE FROM {} WHERE key LIKE $1", self.qualified_table);

        let pattern = format!("{}%", escape_like(prefix));
        let count = self.client.borrow_mut().execute(&sql, &[&pattern])?;
        Ok(count)
    }

    // ==================== Maintenance Operations ====================

    /// Deletes all expired keys.
    ///
    /// Returns the number of keys deleted.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let deleted = store.cleanup_expired()?;
    /// println!("Cleaned up {} expired keys", deleted);
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn cleanup_expired(&self) -> Result<u64> {
        let sql = format!(
            "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at < NOW()",
            self.qualified_table
        );

        let count = self.client.borrow_mut().execute(&sql, &[])?;
        Ok(count)
    }

    /// Deletes all keys.
    ///
    /// Returns the number of keys deleted.
    ///
    /// **Warning**: This will delete all data!
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.clear()?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn clear(&self) -> Result<u64> {
        let sql = format!("DELETE FROM {}", self.qualified_table);
        let count = self.client.borrow_mut().execute(&sql, &[])?;
        Ok(count)
    }

    /// Truncates the table (faster than clear for large datasets).
    ///
    /// **Warning**: This will delete all data!
    pub fn truncate(&self) -> Result<()> {
        let sql = format!("TRUNCATE {}", self.qualified_table);
        self.client.borrow_mut().execute(&sql, &[])?;
        Ok(())
    }

    /// Gets statistics about the store.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// let stats = store.stats()?;
    /// println!("Total keys: {}", stats.total_keys);
    /// println!("Table size: {} bytes", stats.table_size_bytes);
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn stats(&self) -> Result<Stats> {
        let sql = format!(
            r#"
            SELECT
                COUNT(*) as total_keys,
                COUNT(*) FILTER (WHERE expires_at IS NOT NULL AND expires_at < NOW()) as expired_keys,
                COALESCE(SUM(LENGTH(value)), 0)::bigint as total_value_bytes,
                COALESCE(AVG(LENGTH(value)), 0)::float8 as avg_value_bytes,
                COALESCE(MAX(LENGTH(value)), 0)::integer as max_value_bytes
            FROM {}
            "#,
            self.qualified_table
        );

        let mut client = self.client.borrow_mut();
        let row = client.query_one(&sql, &[])?;

        // Get table size
        let size_sql = format!(
            r#"
            SELECT
                pg_total_relation_size('{}') as table_size,
                pg_indexes_size('{}') as index_size
            "#,
            self.qualified_table, self.qualified_table
        );

        let size_row = client.query_one(&size_sql, &[])?;

        Ok(Stats {
            total_keys: row.get::<_, i64>("total_keys") as u64,
            expired_keys: row.get::<_, i64>("expired_keys") as u64,
            total_value_bytes: row.get::<_, i64>("total_value_bytes") as u64,
            avg_value_bytes: row.get::<_, f64>("avg_value_bytes"),
            max_value_bytes: row.get::<_, i32>("max_value_bytes") as u64,
            table_size_bytes: size_row.get::<_, i64>("table_size") as u64,
            index_size_bytes: size_row.get::<_, i64>("index_size") as u64,
        })
    }

    /// Runs VACUUM on the table.
    ///
    /// This reclaims storage space after deletes.
    pub fn vacuum(&self) -> Result<()> {
        let sql = format!("VACUUM {}", self.qualified_table);
        self.client.borrow_mut().execute(&sql, &[])?;
        Ok(())
    }

    /// Runs ANALYZE on the table.
    ///
    /// This updates statistics for the query planner.
    pub fn analyze(&self) -> Result<()> {
        let sql = format!("ANALYZE {}", self.qualified_table);
        self.client.borrow_mut().execute(&sql, &[])?;
        Ok(())
    }

    // ==================== Transaction Support ====================

    /// Executes a function within a transaction.
    ///
    /// The transaction is committed if the function returns `Ok`, rolled back otherwise.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgkv::Store;
    ///
    /// let store = Store::connect("postgresql://localhost/mydb")?;
    /// store.transaction(|store| {
    ///     store.set("key1", b"value1")?;
    ///     store.set("key2", b"value2")?;
    ///     Ok(())
    /// })?;
    /// # Ok::<(), pgkv::Error>(())
    /// ```
    pub fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Self) -> Result<T>,
    {
        self.client.borrow_mut().execute("BEGIN", &[])?;

        match f(self) {
            Ok(result) => {
                self.client.borrow_mut().execute("COMMIT", &[])?;
                Ok(result)
            }
            Err(e) => {
                let _ = self.client.borrow_mut().execute("ROLLBACK", &[]);
                Err(e)
            }
        }
    }

    // ==================== Helper Methods ====================

    fn validate_key(&self, key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidKey {
                reason: "key cannot be empty".into(),
            });
        }

        if key.len() > self.config.max_key_length {
            return Err(Error::InvalidKey {
                reason: format!(
                    "key length {} exceeds maximum {}",
                    key.len(),
                    self.config.max_key_length
                ),
            });
        }

        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<()> {
        if value.len() > self.config.max_value_size {
            return Err(Error::InvalidValue {
                reason: format!(
                    "value size {} exceeds maximum {}",
                    value.len(),
                    self.config.max_value_size
                ),
            });
        }

        Ok(())
    }

    fn row_to_entry(row: &Row) -> Result<Entry> {
        Ok(Entry {
            key: row.get("key"),
            value: row.get("value"),
            expires_at: row.get("expires_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the qualified table name.
    pub fn table_name(&self) -> &str {
        &self.qualified_table
    }
}

/// Escapes special characters for LIKE pattern.
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Unit tests that don't require a database connection

    #[test]
    fn test_escape_like() {
        assert_eq!(escape_like("simple"), "simple");
        assert_eq!(escape_like("with%percent"), "with\\%percent");
        assert_eq!(escape_like("with_underscore"), "with\\_underscore");
        assert_eq!(escape_like("with\\backslash"), "with\\\\backslash");
        assert_eq!(escape_like("combo%_\\"), "combo\\%\\_\\\\");
    }

    #[test]
    fn test_validate_key() {
        let config = Config::new("postgresql://localhost/test").max_key_length(10);

        // We can't fully test without a connection, but we can test the config
        assert_eq!(config.max_key_length, 10);
    }

    #[test]
    fn test_validate_value() {
        let config = Config::new("postgresql://localhost/test").max_value_size(1024);
        assert_eq!(config.max_value_size, 1024);
    }
}
