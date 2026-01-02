//! Type definitions for pgkv.

use std::time::SystemTime;

/// A key-value pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    /// The key.
    pub key: String,
    /// The value as raw bytes.
    pub value: Vec<u8>,
}

impl KeyValue {
    /// Creates a new key-value pair.
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Returns the value as a UTF-8 string, if valid.
    pub fn value_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
    }
}

/// A full entry with metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    /// The key.
    pub key: String,
    /// The value as raw bytes.
    pub value: Vec<u8>,
    /// When the entry expires, if set.
    pub expires_at: Option<SystemTime>,
    /// When the entry was created.
    pub created_at: SystemTime,
    /// When the entry was last updated.
    pub updated_at: SystemTime,
}

impl Entry {
    /// Returns `true` if this entry has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| exp < SystemTime::now())
            .unwrap_or(false)
    }

    /// Returns the time remaining until expiration, if set.
    pub fn ttl(&self) -> Option<std::time::Duration> {
        self.expires_at
            .and_then(|exp| exp.duration_since(SystemTime::now()).ok())
    }

    /// Returns the value as a UTF-8 string, if valid.
    pub fn value_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
    }
}

/// Statistics about the store.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    /// Total number of keys in the store.
    pub total_keys: u64,
    /// Number of expired keys (not yet cleaned up).
    pub expired_keys: u64,
    /// Total size of all values in bytes.
    pub total_value_bytes: u64,
    /// Average value size in bytes.
    pub avg_value_bytes: f64,
    /// Size of the largest value in bytes.
    pub max_value_bytes: u64,
    /// Table size on disk in bytes.
    pub table_size_bytes: u64,
    /// Index size on disk in bytes.
    pub index_size_bytes: u64,
}

/// Options for scanning keys.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Only return keys matching this prefix.
    pub prefix: Option<String>,
    /// Maximum number of keys to return.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
    /// Whether to include expired keys.
    pub include_expired: bool,
}

impl ScanOptions {
    /// Creates new scan options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the prefix filter.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Sets the maximum number of results.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the offset for pagination.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets whether to include expired keys.
    pub fn include_expired(mut self, include: bool) -> Self {
        self.include_expired = include;
        self
    }
}

/// Result of a compare-and-swap operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    /// The swap was successful.
    Success,
    /// The expected value didn't match.
    Mismatch {
        /// The current value of the key.
        current: Option<Vec<u8>>,
    },
    /// The key was not found.
    NotFound,
}

impl CasResult {
    /// Returns `true` if the CAS operation was successful.
    #[inline]
    pub fn is_success(&self) -> bool {
        matches!(self, CasResult::Success)
    }

    /// Returns `true` if there was a mismatch.
    #[inline]
    pub fn is_mismatch(&self) -> bool {
        matches!(self, CasResult::Mismatch { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_key_value() {
        let kv = KeyValue::new("test", b"value".to_vec());
        assert_eq!(kv.key, "test");
        assert_eq!(kv.value, b"value");
        assert_eq!(kv.value_str(), Some("value"));

        let kv_binary = KeyValue::new("binary", vec![0xff, 0xfe]);
        assert!(kv_binary.value_str().is_none());
    }

    #[test]
    fn test_entry_expiration() {
        let now = SystemTime::now();

        // Non-expiring entry
        let entry = Entry {
            key: "test".into(),
            value: vec![],
            expires_at: None,
            created_at: now,
            updated_at: now,
        };
        assert!(!entry.is_expired());
        assert!(entry.ttl().is_none());

        // Expired entry
        let entry = Entry {
            key: "test".into(),
            value: vec![],
            expires_at: Some(now - Duration::from_secs(1)),
            created_at: now,
            updated_at: now,
        };
        assert!(entry.is_expired());
    }

    #[test]
    fn test_scan_options_builder() {
        let opts = ScanOptions::new()
            .prefix("user:")
            .limit(100)
            .offset(50)
            .include_expired(true);

        assert_eq!(opts.prefix, Some("user:".into()));
        assert_eq!(opts.limit, Some(100));
        assert_eq!(opts.offset, Some(50));
        assert!(opts.include_expired);
    }

    #[test]
    fn test_cas_result() {
        assert!(CasResult::Success.is_success());
        assert!(!CasResult::Success.is_mismatch());

        let mismatch = CasResult::Mismatch {
            current: Some(vec![1, 2, 3]),
        };
        assert!(!mismatch.is_success());
        assert!(mismatch.is_mismatch());
    }
}
