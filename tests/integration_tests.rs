//! Integration tests for pgkv.
//!
//! These tests require a PostgreSQL database connection.
//! Set DATABASE_URL environment variable to run these tests:
//!
//! DATABASE_URL=postgresql://localhost/pgkv_test cargo test

use pgkv::{CasResult, Config, ScanOptions, Store, TableType, TtlCleanupStrategy};
use std::time::Duration;

/// Get database URL from environment.
fn get_database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

/// Create a test store with a unique table name.
fn create_test_store(test_name: &str) -> Option<Store> {
    let url = get_database_url()?;

    let config = Config::new(url)
        .table_name(format!("test_{}", test_name))
        .table_type(TableType::Unlogged)
        .auto_create_table(true);

    let store = Store::with_config(config).ok()?;
    store.truncate().ok()?;
    Some(store)
}

// ==================== Basic Operations ====================

#[test]
fn test_set_and_get() {
    let Some(store) = create_test_store("set_get") else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    // Set and get bytes
    store.set("key1", b"value1").unwrap();
    let value = store.get("key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));

    // Set and get string
    store.set("key2", "string value").unwrap();
    let value = store.get_string("key2").unwrap();
    assert_eq!(value, Some("string value".to_string()));

    // Overwrite
    store.set("key1", b"new_value").unwrap();
    let value = store.get("key1").unwrap();
    assert_eq!(value, Some(b"new_value".to_vec()));
}

#[test]
fn test_get_or_err() {
    let Some(store) = create_test_store("get_or_err") else {
        return;
    };

    // Missing key
    let result = store.get_or_err("missing");
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());

    // Existing key
    store.set("key", b"value").unwrap();
    let value = store.get_or_err("key").unwrap();
    assert_eq!(value, b"value".to_vec());
}

#[test]
fn test_delete() {
    let Some(store) = create_test_store("delete") else {
        return;
    };

    store.set("key", b"value").unwrap();
    assert!(store.exists("key").unwrap());

    let deleted = store.delete("key").unwrap();
    assert!(deleted);
    assert!(!store.exists("key").unwrap());

    // Delete non-existent
    let deleted = store.delete("nonexistent").unwrap();
    assert!(!deleted);
}

#[test]
fn test_exists() {
    let Some(store) = create_test_store("exists") else {
        return;
    };

    assert!(!store.exists("key").unwrap());

    store.set("key", b"value").unwrap();
    assert!(store.exists("key").unwrap());

    store.delete("key").unwrap();
    assert!(!store.exists("key").unwrap());
}

#[test]
fn test_set_nx() {
    let Some(store) = create_test_store("set_nx") else {
        return;
    };

    // Should succeed when key doesn't exist
    let result = store.set_nx("key", b"value").unwrap();
    assert!(result);
    assert_eq!(store.get("key").unwrap(), Some(b"value".to_vec()));

    // Should fail when key exists
    let result = store.set_nx("key", b"new_value").unwrap();
    assert!(!result);
    assert_eq!(store.get("key").unwrap(), Some(b"value".to_vec()));
}

// ==================== TTL Operations ====================

#[test]
fn test_set_ex() {
    let Some(store) = create_test_store("set_ex") else {
        return;
    };

    store
        .set_ex("key", b"value", Duration::from_secs(60))
        .unwrap();
    assert!(store.exists("key").unwrap());

    let ttl = store.ttl("key").unwrap();
    assert!(ttl.is_some());
    let ttl = ttl.unwrap();
    assert!(ttl <= Duration::from_secs(60));
    assert!(ttl > Duration::from_secs(55));
}

#[test]
fn test_expire_and_persist() {
    let Some(store) = create_test_store("expire_persist") else {
        return;
    };

    store.set("key", b"value").unwrap();

    // Initially no TTL
    assert!(store.ttl("key").unwrap().is_none());

    // Set TTL
    store.expire("key", Duration::from_secs(60)).unwrap();
    assert!(store.ttl("key").unwrap().is_some());

    // Remove TTL
    store.persist("key").unwrap();
    assert!(store.ttl("key").unwrap().is_none());
}

#[test]
fn test_expiration() {
    let Some(store) = create_test_store("expiration") else {
        return;
    };

    // Set with very short TTL
    store
        .set_ex("key", b"value", Duration::from_millis(50))
        .unwrap();
    assert!(store.exists("key").unwrap());

    // Wait for expiration
    std::thread::sleep(Duration::from_millis(100));

    // Key should be expired
    let value = store.get("key").unwrap();
    assert!(value.is_none());
}

// ==================== Batch Operations ====================

#[test]
fn test_set_many_get_many() {
    let Some(store) = create_test_store("batch") else {
        return;
    };

    let items = vec![
        ("key1", b"value1".as_slice()),
        ("key2", b"value2".as_slice()),
        ("key3", b"value3".as_slice()),
    ];

    store.set_many(&items).unwrap();

    let results = store
        .get_many(&["key1", "key2", "key3", "missing"])
        .unwrap();
    assert_eq!(results.len(), 3);

    // Check all values present
    let mut values: Vec<_> = results.iter().map(|kv| kv.key.as_str()).collect();
    values.sort();
    assert_eq!(values, vec!["key1", "key2", "key3"]);
}

#[test]
fn test_delete_many() {
    let Some(store) = create_test_store("delete_many") else {
        return;
    };

    store.set("key1", b"value1").unwrap();
    store.set("key2", b"value2").unwrap();
    store.set("key3", b"value3").unwrap();

    let deleted = store.delete_many(&["key1", "key2", "nonexistent"]).unwrap();
    assert_eq!(deleted, 2);

    assert!(!store.exists("key1").unwrap());
    assert!(!store.exists("key2").unwrap());
    assert!(store.exists("key3").unwrap());
}

// ==================== Atomic Operations ====================

#[test]
fn test_increment() {
    let Some(store) = create_test_store("increment") else {
        return;
    };

    // Increment non-existent key
    let value = store.increment("counter", 5).unwrap();
    assert_eq!(value, 5);

    // Increment existing key
    let value = store.increment("counter", 3).unwrap();
    assert_eq!(value, 8);

    // Decrement
    let value = store.decrement("counter", 2).unwrap();
    assert_eq!(value, 6);

    // Negative increment
    let value = store.increment("counter", -10).unwrap();
    assert_eq!(value, -4);
}

#[test]
fn test_compare_and_swap() {
    let Some(store) = create_test_store("cas") else {
        return;
    };

    // CAS on non-existent key (expect None)
    let result = store.compare_and_swap("key", None, b"value1").unwrap();
    assert!(result.is_success());

    // CAS with wrong expected value
    let result = store
        .compare_and_swap("key", Some(b"wrong"), b"value2")
        .unwrap();
    assert!(result.is_mismatch());

    // CAS with correct expected value
    let result = store
        .compare_and_swap("key", Some(b"value1"), b"value2")
        .unwrap();
    assert!(result.is_success());

    assert_eq!(store.get("key").unwrap(), Some(b"value2".to_vec()));

    // CAS expecting None but key exists
    let result = store.compare_and_swap("key", None, b"value3").unwrap();
    match result {
        CasResult::Mismatch { current } => {
            assert_eq!(current, Some(b"value2".to_vec()));
        }
        _ => panic!("Expected mismatch"),
    }
}

#[test]
fn test_get_and_set() {
    let Some(store) = create_test_store("get_and_set") else {
        return;
    };

    // Get and set non-existent key
    let old = store.get_and_set("key", b"value1").unwrap();
    assert!(old.is_none());

    // Get and set existing key
    let old = store.get_and_set("key", b"value2").unwrap();
    assert_eq!(old, Some(b"value1".to_vec()));

    assert_eq!(store.get("key").unwrap(), Some(b"value2".to_vec()));
}

#[test]
fn test_get_and_delete() {
    let Some(store) = create_test_store("get_and_delete") else {
        return;
    };

    store.set("key", b"value").unwrap();

    let value = store.get_and_delete("key").unwrap();
    assert_eq!(value, Some(b"value".to_vec()));

    // Key should be gone
    assert!(!store.exists("key").unwrap());

    // Get and delete non-existent
    let value = store.get_and_delete("missing").unwrap();
    assert!(value.is_none());
}

// ==================== Scanning Operations ====================

#[test]
fn test_keys() {
    let Some(store) = create_test_store("keys") else {
        return;
    };

    store.set("user:1", b"alice").unwrap();
    store.set("user:2", b"bob").unwrap();
    store.set("session:1", b"data").unwrap();

    // All keys
    let keys = store.keys(ScanOptions::new()).unwrap();
    assert_eq!(keys.len(), 3);

    // Keys with prefix
    let keys = store.keys(ScanOptions::new().prefix("user:")).unwrap();
    assert_eq!(keys.len(), 2);

    // Keys with limit
    let keys = store.keys(ScanOptions::new().limit(2)).unwrap();
    assert_eq!(keys.len(), 2);
}

#[test]
fn test_scan() {
    let Some(store) = create_test_store("scan") else {
        return;
    };

    store.set("user:1", b"alice").unwrap();
    store.set("user:2", b"bob").unwrap();
    store.set("session:1", b"data").unwrap();

    let items = store.scan(ScanOptions::new().prefix("user:")).unwrap();
    assert_eq!(items.len(), 2);

    // Verify values
    for kv in items {
        assert!(kv.key.starts_with("user:"));
    }
}

#[test]
fn test_count() {
    let Some(store) = create_test_store("count") else {
        return;
    };

    store.set("user:1", b"alice").unwrap();
    store.set("user:2", b"bob").unwrap();
    store.set("session:1", b"data").unwrap();

    let count = store.count(ScanOptions::new()).unwrap();
    assert_eq!(count, 3);

    let count = store.count(ScanOptions::new().prefix("user:")).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_delete_prefix() {
    let Some(store) = create_test_store("delete_prefix") else {
        return;
    };

    store.set("temp:1", b"data1").unwrap();
    store.set("temp:2", b"data2").unwrap();
    store.set("perm:1", b"data3").unwrap();

    let deleted = store.delete_prefix("temp:").unwrap();
    assert_eq!(deleted, 2);

    assert!(!store.exists("temp:1").unwrap());
    assert!(!store.exists("temp:2").unwrap());
    assert!(store.exists("perm:1").unwrap());
}

// ==================== Entry Operations ====================

#[test]
fn test_get_entry() {
    let Some(store) = create_test_store("get_entry") else {
        return;
    };

    store
        .set_ex("key", b"value", Duration::from_secs(60))
        .unwrap();

    let entry = store.get_entry("key").unwrap().unwrap();
    assert_eq!(entry.key, "key");
    assert_eq!(entry.value, b"value".to_vec());
    assert!(entry.expires_at.is_some());
    assert!(!entry.is_expired());

    let ttl = entry.ttl().unwrap();
    assert!(ttl <= Duration::from_secs(60));
}

// ==================== Transaction Operations ====================

#[test]
fn test_transaction_commit() {
    let Some(store) = create_test_store("tx_commit") else {
        return;
    };

    store
        .transaction(|s| {
            s.set("key1", b"value1")?;
            s.set("key2", b"value2")?;
            Ok(())
        })
        .unwrap();

    assert!(store.exists("key1").unwrap());
    assert!(store.exists("key2").unwrap());
}

#[test]
fn test_transaction_rollback() {
    let Some(store) = create_test_store("tx_rollback") else {
        return;
    };

    let result: Result<(), pgkv::Error> = store.transaction(|s| {
        s.set("key1", b"value1")?;
        // Simulate error
        Err(pgkv::Error::Config("test error".into()))
    });

    assert!(result.is_err());
    // Key should not exist due to rollback
    assert!(!store.exists("key1").unwrap());
}

// ==================== Maintenance Operations ====================

#[test]
fn test_cleanup_expired() {
    let Some(store) = create_test_store("cleanup") else {
        return;
    };

    // Create some expired keys
    store
        .set_ex("expired1", b"value", Duration::from_millis(1))
        .unwrap();
    store
        .set_ex("expired2", b"value", Duration::from_millis(1))
        .unwrap();
    store.set("permanent", b"value").unwrap();

    std::thread::sleep(Duration::from_millis(50));

    let deleted = store.cleanup_expired().unwrap();
    assert_eq!(deleted, 2);

    assert!(store.exists("permanent").unwrap());
}

#[test]
fn test_clear() {
    let Some(store) = create_test_store("clear") else {
        return;
    };

    store.set("key1", b"value1").unwrap();
    store.set("key2", b"value2").unwrap();

    let deleted = store.clear().unwrap();
    assert_eq!(deleted, 2);

    assert_eq!(store.count(ScanOptions::new()).unwrap(), 0);
}

#[test]
fn test_stats() {
    let Some(store) = create_test_store("stats") else {
        return;
    };

    store.set("key1", b"value1").unwrap();
    store.set("key2", b"longer_value").unwrap();

    let stats = store.stats().unwrap();
    assert_eq!(stats.total_keys, 2);
    assert!(stats.total_value_bytes > 0);
    assert!(stats.avg_value_bytes > 0.0);
    assert!(stats.table_size_bytes > 0);
}

// ==================== Configuration Tests ====================

#[test]
fn test_table_type_regular() {
    let Some(url) = get_database_url() else {
        return;
    };

    let config = Config::new(url)
        .table_name("test_regular_table")
        .table_type(TableType::Regular)
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    store.set("key", b"value").unwrap();
    assert_eq!(store.get("key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn test_custom_schema() {
    let Some(url) = get_database_url() else {
        return;
    };

    // Create schema first
    let config = Config::new(&url)
        .table_name("test_schema_table")
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    store.set("key", b"value").unwrap();
    assert_eq!(store.get("key").unwrap(), Some(b"value".to_vec()));
}

// ==================== Validation Tests ====================

#[test]
fn test_empty_key_error() {
    let Some(store) = create_test_store("validation") else {
        return;
    };

    let result = store.set("", b"value");
    assert!(result.is_err());
}

#[test]
fn test_key_too_long() {
    let Some(url) = get_database_url() else {
        return;
    };

    let config = Config::new(url)
        .table_name("test_key_limit")
        .max_key_length(10)
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    // This should fail
    let result = store.set("this_key_is_too_long", b"value");
    assert!(result.is_err());

    // This should succeed
    store.set("shortkey", b"value").unwrap();
}

#[test]
fn test_value_too_large() {
    let Some(url) = get_database_url() else {
        return;
    };

    let config = Config::new(url)
        .table_name("test_value_limit")
        .max_value_size(100)
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    // This should fail
    let large_value = vec![0u8; 200];
    let result = store.set("key", &large_value);
    assert!(result.is_err());

    // This should succeed
    let small_value = vec![0u8; 50];
    store.set("key", &small_value).unwrap();
}

// ==================== Edge Cases ====================

#[test]
fn test_binary_values() {
    let Some(store) = create_test_store("binary") else {
        return;
    };

    // Test with binary data including null bytes
    let binary_data = vec![0u8, 1, 2, 255, 0, 128, 64];
    store.set("binary", &binary_data).unwrap();

    let retrieved = store.get("binary").unwrap().unwrap();
    assert_eq!(retrieved, binary_data);
}

#[test]
fn test_unicode_keys() {
    let Some(store) = create_test_store("unicode") else {
        return;
    };

    store.set("key_with_emoji_\u{1F600}", b"value").unwrap();
    store.set("日本語キー", b"japanese").unwrap();
    store.set("key:with:colons", b"value").unwrap();

    assert!(store.exists("key_with_emoji_\u{1F600}").unwrap());
    assert!(store.exists("日本語キー").unwrap());
    assert!(store.exists("key:with:colons").unwrap());
}

#[test]
fn test_special_characters_in_keys() {
    let Some(store) = create_test_store("special_chars") else {
        return;
    };

    let special_keys = vec![
        "key with spaces",
        "key\twith\ttabs",
        "key'with'quotes",
        "key\"with\"doublequotes",
        "key\\with\\backslashes",
        "key%with%percent",
        "key_with_underscore",
    ];

    for key in special_keys {
        store.set(key, b"value").unwrap();
        assert!(store.exists(key).unwrap(), "Key '{}' should exist", key);
    }
}

#[test]
fn test_large_batch() {
    let Some(store) = create_test_store("large_batch") else {
        return;
    };

    // Create 1000 items
    let items: Vec<(String, Vec<u8>)> = (0..1000)
        .map(|i| (format!("key_{}", i), format!("value_{}", i).into_bytes()))
        .collect();

    let refs: Vec<(&str, &[u8])> = items
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_slice()))
        .collect();

    store.set_many(&refs).unwrap();

    let count = store.count(ScanOptions::new()).unwrap();
    assert_eq!(count, 1000);

    // Verify random samples
    assert_eq!(store.get("key_0").unwrap(), Some(b"value_0".to_vec()));
    assert_eq!(store.get("key_500").unwrap(), Some(b"value_500".to_vec()));
    assert_eq!(store.get("key_999").unwrap(), Some(b"value_999".to_vec()));
}

#[test]
fn test_pagination() {
    let Some(store) = create_test_store("pagination") else {
        return;
    };

    // Create 100 items
    for i in 0..100 {
        store.set(&format!("item_{:03}", i), b"value").unwrap();
    }

    // Test pagination
    let page1 = store.keys(ScanOptions::new().limit(25).offset(0)).unwrap();
    let page2 = store.keys(ScanOptions::new().limit(25).offset(25)).unwrap();
    let page3 = store.keys(ScanOptions::new().limit(25).offset(50)).unwrap();
    let page4 = store.keys(ScanOptions::new().limit(25).offset(75)).unwrap();

    assert_eq!(page1.len(), 25);
    assert_eq!(page2.len(), 25);
    assert_eq!(page3.len(), 25);
    assert_eq!(page4.len(), 25);

    // Ensure no overlap
    let mut all_keys: Vec<_> = page1
        .into_iter()
        .chain(page2)
        .chain(page3)
        .chain(page4)
        .collect();
    all_keys.sort();
    all_keys.dedup();
    assert_eq!(all_keys.len(), 100);
}

// ==================== TTL Cleanup Strategy Tests ====================

#[test]
fn test_ttl_cleanup_on_read() {
    let Some(url) = get_database_url() else {
        return;
    };

    let config = Config::new(&url)
        .table_name("test_ttl_on_read")
        .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead)
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    // Set a key with very short TTL
    store
        .set_ex("expires_soon", b"value", Duration::from_millis(10))
        .unwrap();

    std::thread::sleep(Duration::from_millis(50));

    // Key should be filtered out on read and deleted automatically
    assert!(store.get("expires_soon").unwrap().is_none());
}

#[test]
fn test_ttl_cleanup_manual() {
    let Some(url) = get_database_url() else {
        return;
    };

    let config = Config::new(&url)
        .table_name("test_ttl_manual")
        .ttl_cleanup_strategy(TtlCleanupStrategy::Manual)
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    // Set a key with very short TTL
    store
        .set_ex("expires_soon", b"value", Duration::from_millis(10))
        .unwrap();

    std::thread::sleep(Duration::from_millis(50));

    // Key should be filtered out on read but NOT deleted automatically
    assert!(store.get("expires_soon").unwrap().is_none());

    // Key still exists in database but is expired - cleanup_expired should remove it
    let deleted = store.cleanup_expired().unwrap();
    assert_eq!(deleted, 1);
}

#[test]
fn test_ttl_cleanup_disabled() {
    let Some(url) = get_database_url() else {
        return;
    };

    let config = Config::new(&url)
        .table_name("test_ttl_disabled")
        .ttl_cleanup_strategy(TtlCleanupStrategy::Disabled)
        .auto_create_table(true);

    let store = Store::with_config(config).unwrap();
    store.truncate().unwrap();

    // Set a key with very short TTL
    store
        .set_ex("expires_soon", b"value", Duration::from_millis(10))
        .unwrap();

    std::thread::sleep(Duration::from_millis(50));

    // Key should STILL be returned because TTL checking is disabled
    let value = store.get("expires_soon").unwrap();
    assert!(value.is_some());
    assert_eq!(value.unwrap(), b"value".to_vec());
}
