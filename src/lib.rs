//! # pgkv - PostgreSQL Key-Value Store
//!
//! A high-performance, production-grade key-value store backed by PostgreSQL unlogged tables.
//!
//! ## Features
//!
//! - **High Performance**: Uses PostgreSQL UNLOGGED tables for maximum write throughput
//! - **Runtime Agnostic**: Synchronous API works with any async runtime or none at all
//! - **Minimal Dependencies**: Only depends on `postgres` and `thiserror`
//! - **Rich API**: Comprehensive operations including batch, atomic, TTL, and prefix scanning
//! - **Type Safe**: Strong typing with optional serde support for automatic serialization
//! - **Production Ready**: Comprehensive error handling, connection pooling support, and transaction safety
//! - **Configurable TTL Cleanup**: Choose between automatic, manual, or disabled key expiration cleanup
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use pgkv::{Store, Config};
//!
//! // Create a store with default configuration
//! let store = Store::connect("postgresql://umesh@localhost/postgres")?;
//!
//! // Basic operations
//! store.set("key", b"value")?;
//! let value = store.get("key")?;
//! store.delete("key")?;
//!
//! // Batch operations
//! store.set_many(&[("k1", b"v1".as_slice()), ("k2", b"v2")])?;
//! let values = store.get_many(&["k1", "k2"])?;
//!
//! // TTL support
//! use std::time::Duration;
//! store.set_ex("temp", b"expires", Duration::from_secs(60))?;
//!
//! // Atomic operations
//! store.compare_and_swap("counter", Some(b"old"), b"new")?;
//! store.increment("counter", 1)?;
//! # Ok::<(), pgkv::Error>(())
//! ```
//!
//! ## Why Unlogged Tables?
//!
//! PostgreSQL UNLOGGED tables provide significantly higher write performance by skipping
//! write-ahead logging (WAL). This makes them ideal for:
//!
//! - **Caching**: Data that can be regenerated if lost
//! - **Session storage**: Ephemeral user session data
//! - **Rate limiting**: Counters and temporary state
//! - **Job queues**: Transient task data
//!
//! **Trade-off**: Data in UNLOGGED tables is not crash-safe and will be truncated after
//! an unclean shutdown. Use regular tables if you need durability.
//!
//! ## Architecture
//!
//! The library creates a simple schema:
//!
//! ```sql
//! CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
//!     key TEXT PRIMARY KEY,
//!     value BYTEA NOT NULL,
//!     expires_at TIMESTAMPTZ,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
//! );
//! ```
//!
//! ## Configuration
//!
//! ```rust,no_run
//! use pgkv::{Store, Config, TableType, TtlCleanupStrategy};
//!
//! let config = Config::new("postgresql://umesh@localhost/postgres")
//!     .table_name("my_cache")
//!     .table_type(TableType::Unlogged)
//!     .auto_create_table(true)
//!     .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead);
//!
//! let store = Store::with_config(config)?;
//! # Ok::<(), pgkv::Error>(())
//! ```
//!
//! ## TTL Cleanup Strategies
//!
//! The library provides three strategies for handling expired keys:
//!
//! ```rust,no_run
//! use pgkv::{Config, TtlCleanupStrategy};
//!
//! // Option 1: Automatic cleanup on read (default)
//! // Expired keys are deleted when accessed
//! let config = Config::new("postgresql://umesh@localhost/postgres")
//!     .ttl_cleanup_strategy(TtlCleanupStrategy::OnRead);
//!
//! // Option 2: Manual cleanup - YOU control when cleanup happens
//! // Call store.cleanup_expired() on your own schedule (cron, background task, etc.)
//! let config = Config::new("postgresql://umesh@localhost/postgres")
//!     .ttl_cleanup_strategy(TtlCleanupStrategy::Manual);
//!
//! // Option 3: Disabled - no expiration checking at all
//! // Use when you don't use TTL features and want maximum performance
//! let config = Config::new("postgresql://umesh@localhost/postgres")
//!     .ttl_cleanup_strategy(TtlCleanupStrategy::Disabled);
//! # Ok::<(), pgkv::Error>(())
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![deny(unsafe_code)]

mod config;
mod error;
mod store;
mod types;

#[cfg(feature = "serde")]
mod serde_support;

pub use config::{Config, TableType, TtlCleanupStrategy};
pub use error::{Error, Result};
pub use store::Store;
pub use types::{CasResult, Entry, KeyValue, ScanOptions, Stats};

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
pub use serde_support::{TypedStore, TypedStoreExt};

/// Prelude module for convenient imports.
///
/// ```rust,no_run
/// use pgkv::prelude::*;
/// ```
pub mod prelude {
    pub use crate::config::{Config, TableType, TtlCleanupStrategy};
    pub use crate::error::{Error, Result};
    pub use crate::store::Store;
    pub use crate::types::{CasResult, Entry, KeyValue, ScanOptions, Stats};

    #[cfg(feature = "serde")]
    pub use crate::serde_support::{TypedStore, TypedStoreExt};
}
