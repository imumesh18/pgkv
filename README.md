# pgkv

A high-performance, production-grade key-value store backed by PostgreSQL unlogged tables.

[![Crates.io](https://img.shields.io/crates/v/pgkv.svg)](https://crates.io/crates/pgkv)
[![Documentation](https://docs.rs/pgkv/badge.svg)](https://docs.rs/pgkv)
[![CI](https://github.com/yourusername/pgkv/workflows/CI/badge.svg)](https://github.com/yourusername/pgkv/actions)
[![License](https://img.shields.io/crates/l/pgkv.svg)](LICENSE)

## Features

- **High Performance**: Uses PostgreSQL UNLOGGED tables for maximum write throughput (2-3x faster than regular tables)
- **Runtime Agnostic**: Synchronous API works with any async runtime or none at all
- **Minimal Dependencies**: Only depends on `postgres` and `thiserror` - no async runtime required for your code
- **Rich API**: Comprehensive operations including batch, atomic, TTL, and prefix scanning
- **Type Safe**: Strong typing with optional serde support for automatic serialization
- **Production Ready**: Comprehensive error handling, connection pooling support, and transaction safety
- **Configurable TTL Cleanup**: Choose automatic, manual, or disabled expiration handling
- **Zero Unsafe Code**: 100% safe Rust

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
pgkv = "0.1"

# Optional: Enable serde support for automatic serialization
# pgkv = { version = "0.1", features = ["serde"] }
```

Basic usage:

```rust
use pgkv::{Store, Config};

fn main() -> pgkv::Result<()> {
    // Connect to PostgreSQL
    let store = Store::connect("postgresql://localhost/mydb")?;

    // Basic CRUD operations
    store.set("user:1:name", b"Alice")?;

    if let Some(value) = store.get("user:1:name")? {
        println!("Name: {}", String::from_utf8_lossy(&value));
    }

    store.delete("user:1:name")?;

    Ok(())
}
```

## Why Unlogged Tables?

PostgreSQL UNLOGGED tables provide significantly higher write performance by skipping write-ahead logging (WAL). This makes them ideal for:

- **Caching**: Data that can be regenerated if lost
- **Session storage**: Ephemeral user session data
- **Rate limiting**: Counters and temporary state
- **Job queues**: Transient task data
- **Feature flags**: Temporary configuration

**Trade-off**: Data in UNLOGGED tables is not crash-safe and will be truncated after an unclean shutdown. Use regular tables (`TableType::Regular`) if you need durability.

## API Overview

### Basic Operations

```rust
use pgkv::Store;

let store = Store::connect("postgresql://localhost/mydb")?;

// Set/Get/Delete
store.set("key", b"value")?;
store.set("key", "string value")?;  // Also accepts &str
let value = store.get("key")?;       // Returns Option<Vec<u8>>
let string = store.get_string("key")?; // Returns Option<String>
store.delete("key")?;

// Check existence
if store.exists("key")? {
    println!("Key exists");
}

// Set only if key doesn't exist
if store.set_nx("key", b"value")? {
    println!("Key was created");
}
```

### TTL (Time-To-Live) Support

```rust
use std::time::Duration;

// Set with expiration
store.set_ex("session", b"data", Duration::from_secs(3600))?;

// Update TTL on existing key
store.expire("session", Duration::from_secs(7200))?;

// Check remaining TTL
if let Some(ttl) = store.ttl("session")? {
    println!("Expires in {:?}", ttl);
}

// Remove expiration (make persistent)
store.persist("session")?;

// Cleanup all expired keys
let cleaned = store.cleanup_expired()?;
```

### TTL Cleanup Strategies

You can configure how expired keys are handled:

```rust
use pgkv::{Config, TtlCleanupStrategy};

// Automatic cleanup on read (default)
// Expired keys are deleted when accessed
let config = Config::new("postgresql://localhost/mydb")
    .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead);

// Manual cleanup - you control when expired keys are deleted
// Call store.cleanup_expired() on your own schedule (e.g., via cron)
let config = Config::new("postgresql://localhost/mydb")
    .ttl_cleanup_strategy(TtlCleanupStrategy::Manual);

// Disabled - TTL is ignored entirely (maximum read performance)
// Expired keys are returned as if still valid
let config = Config::new("postgresql://localhost/mydb")
    .ttl_cleanup_strategy(TtlCleanupStrategy::Disabled);
```

### Batch Operations

```rust
// Set multiple keys atomically
store.set_many(&[
    ("key1", b"value1".as_slice()),
    ("key2", b"value2"),
    ("key3", b"value3"),
])?;

// Get multiple keys
let results = store.get_many(&["key1", "key2", "key3"])?;
for kv in results {
    println!("{}: {:?}", kv.key, kv.value);
}

// Delete multiple keys
let deleted = store.delete_many(&["key1", "key2"])?;
```

### Atomic Operations

```rust
use pgkv::CasResult;

// Atomic increment/decrement
let count = store.increment("counter", 1)?;
let count = store.decrement("counter", 1)?;

// Compare-and-swap
match store.compare_and_swap("key", Some(b"old_value"), b"new_value")? {
    CasResult::Success => println!("Updated"),
    CasResult::Mismatch { current } => println!("Value changed: {:?}", current),
    CasResult::NotFound => println!("Key doesn't exist"),
}

// Get and set atomically
let old_value = store.get_and_set("key", b"new_value")?;

// Get and delete atomically
let value = store.get_and_delete("key")?;
```

### Prefix Scanning

```rust
use pgkv::ScanOptions;

// List keys with prefix
let keys = store.keys(ScanOptions::new().prefix("user:"))?;

// Scan key-value pairs with pagination
let items = store.scan(
    ScanOptions::new()
        .prefix("user:")
        .limit(100)
        .offset(0)
)?;

// Count keys matching pattern
let count = store.count(ScanOptions::new().prefix("session:"))?;

// Delete all keys with prefix
let deleted = store.delete_prefix("temp:")?;
```

### Transactions

```rust
store.transaction(|s| {
    s.set("key1", b"value1")?;
    s.set("key2", b"value2")?;
    // If any operation fails, all changes are rolled back
    Ok(())
})?;
```

### Configuration

```rust
use pgkv::{Config, TableType, TtlCleanupStrategy, Store};

let config = Config::new("postgresql://localhost/mydb")
    .table_name("my_cache")                      // Custom table name
    .table_type(TableType::Unlogged)             // Or TableType::Regular for durability
    .auto_create_table(true)                     // Auto-create table on connect
    .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead) // TTL handling strategy
    .max_key_length(1024)                        // Max key size in bytes
    .max_value_size(100 * 1024 * 1024)           // Max value size (100MB)
    .schema("custom_schema")                     // Custom schema (default: public)
    .application_name("my_app");                 // Shows in pg_stat_activity

let store = Store::with_config(config)?;
```

### Typed Store (with serde feature)

```rust
use pgkv::{Store, TypedStore};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct User {
    name: String,
    email: String,
}

let store = Store::connect("postgresql://localhost/mydb")?;
let users: TypedStore<User> = TypedStore::new(&store);

// Automatically serializes to JSON
users.set("user:1", &User {
    name: "Alice".into(),
    email: "alice@example.com".into(),
})?;

// Automatically deserializes
let user: Option<User> = users.get("user:1")?;
```

## Database Schema

The library creates the following table structure:

```sql
CREATE UNLOGGED TABLE IF NOT EXISTS kv_store (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for efficient expiration cleanup
CREATE INDEX IF NOT EXISTS kv_store_expires_idx
    ON kv_store (expires_at)
    WHERE expires_at IS NOT NULL;
```

## Thread Safety

`Store` is `Send` but not `Sync` due to the use of `RefCell` for interior mutability. For multi-threaded access:

1. **Connection pooling** (recommended): Use a pool like `r2d2` or `deadpool` with separate `Store` instances per thread
2. **Mutex wrapping**: Wrap `Store` in `Mutex<Store>` for shared access

```rust
use std::sync::Mutex;

let store = Mutex::new(Store::connect("postgresql://localhost/mydb")?);

// In each thread:
let guard = store.lock().unwrap();
guard.set("key", b"value")?;
```

## Benchmarks

Run benchmarks comparing PostgreSQL UNLOGGED, PostgreSQL Regular, and Redis:

```bash
# PostgreSQL only
DATABASE_URL=postgresql://user@localhost/postgres cargo bench

# With Redis comparison
DATABASE_URL=postgresql://user@localhost/postgres REDIS_URL=redis://localhost:6379 cargo bench
```

Benchmark groups:
- `set` - Single key writes with various value sizes (64B - 4KB)
- `get` - Single key reads (existing and missing keys)
- `set_many` / `get_many` - Batch operations (10 - 500 keys)
- `delete` - Single key deletes
- `exists` - Key existence checks
- `increment` - Atomic counter increments
- `set_with_ttl` - Writes with TTL
- `scan` - Prefix scanning with pagination
- `mixed_workload` - 80% reads / 20% writes

*Results vary by hardware and PostgreSQL configuration. Run on your system for accurate numbers.*

Benchmarks run automatically every week. See `.github/workflows/benchmark.yml`.

## Comparison with Alternatives

| Feature | pgkv | Redis | memcached |
|---------|------|-------|-----------|
| ACID Transactions | Yes | Limited | No |
| SQL Queries | Yes (via raw SQL) | No | No |
| TTL Support | Yes | Yes | Yes |
| Persistence | Optional | Optional | No |
| Clustering | Via PG | Yes | Yes |
| External Service | Uses existing PG | Yes | Yes |
| Memory Limit | Disk-based | Memory | Memory |

**When to use pgkv:**
- You already have PostgreSQL and want to avoid adding Redis/memcached
- You need ACID guarantees for some operations
- Your cache can fit on disk (not purely in-memory)
- You want SQL-level access to cached data for debugging

**When to use Redis/memcached:**
- You need sub-millisecond latency
- Your workload is purely in-memory
- You need built-in clustering
- You need advanced data structures (sorted sets, streams, etc.)

## Error Handling

```rust
use pgkv::{Store, Error};

let store = Store::connect("postgresql://localhost/mydb")?;

match store.get("key") {
    Ok(Some(value)) => println!("Found: {:?}", value),
    Ok(None) => println!("Key not found"),
    Err(Error::Connection(msg)) => eprintln!("Connection failed: {}", msg),
    Err(Error::Query(msg)) => eprintln!("Query failed: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}

// Error predicates
if let Err(e) = store.get_or_err("missing") {
    if e.is_not_found() {
        println!("Key doesn't exist");
    }
    if e.is_recoverable() {
        println!("Can retry this operation");
    }
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
