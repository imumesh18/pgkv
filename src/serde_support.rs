//! Serde support for type-safe key-value operations.
//!
//! This module is only available when the `serde` feature is enabled.

use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::store::Store;
use crate::types::ScanOptions;

/// A typed wrapper around [`Store`] that automatically serializes/deserializes values.
///
/// This provides a type-safe API for working with structured data.
///
/// # Example
///
/// ```rust,no_run
/// use pgkv::{Store, TypedStore};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct User {
///     name: String,
///     email: String,
/// }
///
/// let store = Store::connect("postgresql://localhost/mydb")?;
/// let typed: TypedStore<User> = TypedStore::new(&store);
///
/// // Store a user
/// typed.set("user:1", &User {
///     name: "Alice".to_string(),
///     email: "alice@example.com".to_string(),
/// })?;
///
/// // Retrieve and automatically deserialize
/// let user: Option<User> = typed.get("user:1")?;
/// # Ok::<(), pgkv::Error>(())
/// ```
pub struct TypedStore<'a, T> {
    store: &'a Store,
    _phantom: PhantomData<T>,
}

impl<'a, T> TypedStore<'a, T>
where
    T: Serialize + DeserializeOwned,
{
    /// Creates a new typed store wrapper.
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    /// Gets a value by key and deserializes it.
    ///
    /// Returns `None` if the key doesn't exist or has expired.
    pub fn get(&self, key: &str) -> Result<Option<T>> {
        match self.store.get(key)? {
            Some(bytes) => {
                let value: T = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Gets a value by key, returning an error if not found.
    pub fn get_or_err(&self, key: &str) -> Result<T> {
        self.get(key)?.ok_or_else(|| Error::NotFound {
            key: key.to_string(),
        })
    }

    /// Sets a value by key, serializing it to JSON.
    pub fn set(&self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        self.store.set(key, &bytes)
    }

    /// Sets a value with an expiration time.
    pub fn set_ex(&self, key: &str, value: &T, ttl: Duration) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        self.store.set_ex(key, &bytes, ttl)
    }

    /// Sets a value only if the key doesn't exist.
    pub fn set_nx(&self, key: &str, value: &T) -> Result<bool> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        self.store.set_nx(key, &bytes)
    }

    /// Sets a value with TTL only if the key doesn't exist.
    pub fn set_nx_ex(&self, key: &str, value: &T, ttl: Duration) -> Result<bool> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        self.store.set_nx_ex(key, &bytes, ttl)
    }

    /// Gets multiple values and deserializes them.
    pub fn get_many(&self, keys: &[&str]) -> Result<Vec<(String, T)>> {
        let kvs = self.store.get_many(keys)?;
        let mut results = Vec::with_capacity(kvs.len());

        for kv in kvs {
            let value: T = serde_json::from_slice(&kv.value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            results.push((kv.key, value));
        }

        Ok(results)
    }

    /// Sets multiple key-value pairs.
    pub fn set_many(&self, items: &[(&str, &T)]) -> Result<()> {
        let serialized: Vec<(&str, Vec<u8>)> = items
            .iter()
            .map(|(k, v)| {
                let bytes =
                    serde_json::to_vec(v).map_err(|e| Error::Serialization(e.to_string()))?;
                Ok((*k, bytes))
            })
            .collect::<Result<Vec<_>>>()?;

        let refs: Vec<(&str, &[u8])> = serialized.iter().map(|(k, v)| (*k, v.as_slice())).collect();
        self.store.set_many(&refs)
    }

    /// Scans key-value pairs with optional filtering.
    pub fn scan(&self, options: ScanOptions) -> Result<Vec<(String, T)>> {
        let kvs = self.store.scan(options)?;
        let mut results = Vec::with_capacity(kvs.len());

        for kv in kvs {
            let value: T = serde_json::from_slice(&kv.value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            results.push((kv.key, value));
        }

        Ok(results)
    }

    /// Gets the current value and sets a new value atomically.
    pub fn get_and_set(&self, key: &str, value: &T) -> Result<Option<T>> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;

        match self.store.get_and_set(key, &bytes)? {
            Some(old_bytes) => {
                let old_value: T = serde_json::from_slice(&old_bytes)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(old_value))
            }
            None => Ok(None),
        }
    }

    /// Gets the current value and deletes the key atomically.
    pub fn get_and_delete(&self, key: &str) -> Result<Option<T>> {
        match self.store.get_and_delete(key)? {
            Some(bytes) => {
                let value: T = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Returns a reference to the underlying store.
    pub fn store(&self) -> &Store {
        self.store
    }
}

/// Extension trait for convenient typed access.
pub trait TypedStoreExt {
    /// Creates a typed store wrapper for the given type.
    fn typed<T: Serialize + DeserializeOwned>(&self) -> TypedStore<'_, T>;
}

impl TypedStoreExt for Store {
    fn typed<T: Serialize + DeserializeOwned>(&self) -> TypedStore<'_, T> {
        TypedStore::new(self)
    }
}

#[cfg(test)]
mod tests {
    // Integration tests would require a database connection
    // Unit tests for serialization logic

    use super::*;

    #[test]
    fn test_serialization_error_display() {
        let err = Error::Serialization("test error".to_string());
        assert!(err.to_string().contains("serialization"));
    }
}
