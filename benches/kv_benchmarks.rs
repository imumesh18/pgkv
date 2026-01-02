//! Comprehensive benchmarks for pgkv comparing:
//! - PostgreSQL UNLOGGED tables (pgkv default - fast, non-durable)
//! - PostgreSQL Regular tables (slower, durable)
//! - Redis (in-memory key-value store)
//!
//! ## Running Benchmarks
//!
//! ```bash
//! # Run all benchmarks
//! cargo bench
//!
//! # Run specific benchmark group
//! cargo bench -- set
//! cargo bench -- get
//! cargo bench -- mixed
//!
//! # With custom database URL
//! DATABASE_URL=postgresql://umesh@localhost/postgres cargo bench
//!
//! # With Redis (optional)
//! REDIS_URL=redis://localhost:6379 cargo bench
//! ```
//!
//! ## Benchmark Categories
//!
//! - **set**: Single key write performance at various value sizes
//! - **get**: Single key read performance (existing vs missing keys)
//! - **get_many**: Batch read performance at various batch sizes
//! - **set_many**: Batch write performance at various batch sizes
//! - **delete**: Single key deletion performance
//! - **exists**: Key existence check performance
//! - **increment**: Atomic counter increment performance
//! - **set_with_ttl**: Write with TTL expiration
//! - **scan**: Prefix-based key scanning at various limits
//! - **mixed_workload**: Realistic read/write mixed workloads
//! - **throughput**: Sustained throughput measurement
//! - **large_values**: Performance with large value sizes

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use pgkv::{Config, Store, TableType, TtlCleanupStrategy};
use rand::Rng;
use std::time::Duration;

/// Default database URL
const DEFAULT_DATABASE_URL: &str = "postgresql://umesh@localhost/postgres";

/// Number of keys for pre-population
const NUM_PREPOPULATED_KEYS: u64 = 1000;

/// Default measurement time for benchmarks
const MEASUREMENT_TIME_SECS: u64 = 10;

/// Extended measurement time for throughput tests
const THROUGHPUT_MEASUREMENT_TIME_SECS: u64 = 15;

/// Get database URL from environment or use default.
fn get_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string())
}

/// Get Redis URL from environment.
fn get_redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

/// Create a test store with PostgreSQL UNLOGGED table.
fn create_unlogged_store(name: &str) -> Option<Store> {
    let config = Config::new(get_database_url())
        .table_name(format!("bench_unlogged_{}", name))
        .table_type(TableType::Unlogged)
        .ttl_cleanup_strategy(TtlCleanupStrategy::Manual) // Disable auto-cleanup for benchmarks
        .auto_create_table(true);

    let store = Store::with_config(config).ok()?;
    store.truncate().ok()?;
    Some(store)
}

/// Create a test store with PostgreSQL Regular (logged) table.
fn create_regular_store(name: &str) -> Option<Store> {
    let config = Config::new(get_database_url())
        .table_name(format!("bench_regular_{}", name))
        .table_type(TableType::Regular)
        .ttl_cleanup_strategy(TtlCleanupStrategy::Manual) // Disable auto-cleanup for benchmarks
        .auto_create_table(true);

    let store = Store::with_config(config).ok()?;
    store.truncate().ok()?;
    store.truncate().ok()?;
    Some(store)
}

/// Create a Redis connection.
fn create_redis_connection() -> Option<redis::Connection> {
    let url = get_redis_url()?;
    let client = redis::Client::open(url).ok()?;
    client.get_connection().ok()
}

/// Generate random bytes of given size.
fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.r#gen()).collect()
}

/// Generate a key with prefix and index.
fn make_key(prefix: &str, i: u64) -> String {
    format!("{}_{}", prefix, i)
}

/// Pre-populate a store with test data.
fn prepopulate_store(store: &Store, prefix: &str, count: u64, value: &[u8]) {
    for i in 0..count {
        store.set(&make_key(prefix, i), value).unwrap();
    }
}

/// Pre-populate Redis with test data.
fn prepopulate_redis(conn: &mut redis::Connection, prefix: &str, count: u64, value: &[u8]) {
    for i in 0..count {
        let _: () = redis::cmd("SET")
            .arg(make_key(prefix, i))
            .arg(value)
            .query(conn)
            .unwrap();
    }
}

/// Cleanup Redis database.
fn cleanup_redis(conn: &mut redis::Connection) {
    let _: () = redis::cmd("FLUSHDB").query(conn).unwrap_or(());
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    // Test various value sizes: 64B, 256B, 1KB, 4KB, 16KB
    let value_sizes = [64, 256, 1024, 4096, 16384];

    for size in value_sizes {
        let value = random_bytes(size);
        group.throughput(Throughput::Bytes(size as u64));

        // PostgreSQL UNLOGGED
        if let Some(store) = create_unlogged_store(&format!("set_{}", size)) {
            group.bench_with_input(BenchmarkId::new("pg_unlogged", size), &size, |b, _| {
                let mut i = 0u64;
                b.iter(|| {
                    let key = make_key("key", i);
                    i += 1;
                    store.set(black_box(&key), black_box(&value)).unwrap();
                });
            });
        }

        // PostgreSQL Regular (logged)
        if let Some(store) = create_regular_store(&format!("set_{}", size)) {
            group.bench_with_input(BenchmarkId::new("pg_regular", size), &size, |b, _| {
                let mut i = 0u64;
                b.iter(|| {
                    let key = make_key("key", i);
                    i += 1;
                    store.set(black_box(&key), black_box(&value)).unwrap();
                });
            });
        }

        // Redis
        if let Some(mut conn) = create_redis_connection() {
            let prefix = format!("bench_set_{}", size);
            group.bench_with_input(BenchmarkId::new("redis", size), &size, |b, _| {
                let mut i = 0u64;
                b.iter(|| {
                    let key = make_key(&prefix, i);
                    i += 1;
                    let _: () = redis::cmd("SET")
                        .arg(black_box(&key))
                        .arg(black_box(&value))
                        .query(&mut conn)
                        .unwrap();
                });
            });
            cleanup_redis(&mut conn);
        }
    }

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let value = random_bytes(256);

    // PostgreSQL UNLOGGED
    if let Some(store) = create_unlogged_store("get") {
        prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("pg_unlogged/existing_key", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                i += 1;
                black_box(store.get(black_box(&key)).unwrap());
            });
        });

        group.bench_function("pg_unlogged/missing_key", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("missing", i);
                i += 1;
                black_box(store.get(black_box(&key)).unwrap());
            });
        });
    }

    // PostgreSQL Regular
    if let Some(store) = create_regular_store("get") {
        prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("pg_regular/existing_key", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                i += 1;
                black_box(store.get(black_box(&key)).unwrap());
            });
        });

        group.bench_function("pg_regular/missing_key", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("missing", i);
                i += 1;
                black_box(store.get(black_box(&key)).unwrap());
            });
        });
    }

    // Redis
    if let Some(mut conn) = create_redis_connection() {
        let prefix = "bench_get";
        prepopulate_redis(&mut conn, prefix, NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("redis/existing_key", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                i += 1;
                let _: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(black_box(&key))
                    .query(&mut conn)
                    .unwrap();
            });
        });

        group.bench_function("redis/missing_key", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("missing", i);
                i += 1;
                let _: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(black_box(&key))
                    .query(&mut conn)
                    .unwrap();
            });
        });

        cleanup_redis(&mut conn);
    }

    group.finish();
}

// GET_MANY Benchmarks - Batch Reads

fn bench_get_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_many");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let batch_sizes = [10, 50, 100, 500];
    let value = random_bytes(256);

    for batch_size in batch_sizes {
        group.throughput(Throughput::Elements(batch_size as u64));

        // PostgreSQL UNLOGGED
        if let Some(store) = create_unlogged_store(&format!("get_many_{}", batch_size)) {
            prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

            let keys: Vec<String> = (0..batch_size).map(|i| make_key("key", i as u64)).collect();
            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

            group.bench_with_input(
                BenchmarkId::new("pg_unlogged", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        black_box(store.get_many(black_box(&key_refs)).unwrap());
                    });
                },
            );
        }

        // PostgreSQL Regular
        if let Some(store) = create_regular_store(&format!("get_many_{}", batch_size)) {
            prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

            let keys: Vec<String> = (0..batch_size).map(|i| make_key("key", i as u64)).collect();
            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

            group.bench_with_input(
                BenchmarkId::new("pg_regular", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        black_box(store.get_many(black_box(&key_refs)).unwrap());
                    });
                },
            );
        }

        // Redis MGET
        if let Some(mut conn) = create_redis_connection() {
            let prefix = format!("bench_mget_{}", batch_size);
            prepopulate_redis(&mut conn, &prefix, NUM_PREPOPULATED_KEYS, &value);

            let keys: Vec<String> = (0..batch_size)
                .map(|i| make_key(&prefix, i as u64))
                .collect();

            group.bench_with_input(
                BenchmarkId::new("redis", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        let _: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                            .arg(black_box(&keys))
                            .query(&mut conn)
                            .unwrap();
                    });
                },
            );

            cleanup_redis(&mut conn);
        }
    }

    group.finish();
}

// SET_MANY Benchmarks - Batch Writes

fn bench_set_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_many");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let batch_sizes = [10, 50, 100, 500];

    for batch_size in batch_sizes {
        group.throughput(Throughput::Elements(batch_size as u64));

        let items: Vec<(String, Vec<u8>)> = (0..batch_size)
            .map(|i| (make_key("key", i as u64), random_bytes(256)))
            .collect();

        // PostgreSQL UNLOGGED
        if let Some(store) = create_unlogged_store(&format!("set_many_{}", batch_size)) {
            group.bench_with_input(
                BenchmarkId::new("pg_unlogged", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        let refs: Vec<(&str, &[u8])> = items
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.as_slice()))
                            .collect();
                        store.set_many(black_box(&refs)).unwrap();
                    });
                },
            );
        }

        // PostgreSQL Regular
        if let Some(store) = create_regular_store(&format!("set_many_{}", batch_size)) {
            group.bench_with_input(
                BenchmarkId::new("pg_regular", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        let refs: Vec<(&str, &[u8])> = items
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.as_slice()))
                            .collect();
                        store.set_many(black_box(&refs)).unwrap();
                    });
                },
            );
        }

        // Redis MSET
        if let Some(mut conn) = create_redis_connection() {
            let prefix = format!("bench_mset_{}", batch_size);
            let redis_items: Vec<(String, Vec<u8>)> = (0..batch_size)
                .map(|i| (make_key(&prefix, i as u64), random_bytes(256)))
                .collect();

            group.bench_with_input(
                BenchmarkId::new("redis", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        let mut cmd = redis::cmd("MSET");
                        for (k, v) in &redis_items {
                            cmd.arg(k).arg(v);
                        }
                        let _: () = cmd.query(&mut conn).unwrap();
                    });
                },
            );

            cleanup_redis(&mut conn);
        }
    }

    group.finish();
}

// DELETE Benchmarks

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let value = b"test_value";

    // PostgreSQL UNLOGGED
    if let Some(store) = create_unlogged_store("delete") {
        group.bench_function("pg_unlogged", |b| {
            let mut i = 0u64;
            b.iter_custom(|iters| {
                // Setup: create keys to delete
                for j in 0..iters {
                    store.set(&make_key("del_key", i + j), value).unwrap();
                }

                // Benchmark: delete them
                let start = std::time::Instant::now();
                for j in 0..iters {
                    store.delete(&make_key("del_key", i + j)).unwrap();
                }
                i += iters;
                start.elapsed()
            });
        });
    }

    // PostgreSQL Regular
    if let Some(store) = create_regular_store("delete") {
        group.bench_function("pg_regular", |b| {
            let mut i = 0u64;
            b.iter_custom(|iters| {
                for j in 0..iters {
                    store.set(&make_key("del_key", i + j), value).unwrap();
                }

                let start = std::time::Instant::now();
                for j in 0..iters {
                    store.delete(&make_key("del_key", i + j)).unwrap();
                }
                i += iters;
                start.elapsed()
            });
        });
    }

    // Redis
    if let Some(mut conn) = create_redis_connection() {
        group.bench_function("redis", |b| {
            let mut i = 0u64;
            b.iter_custom(|iters| {
                for j in 0..iters {
                    let _: () = redis::cmd("SET")
                        .arg(make_key("del_key", i + j))
                        .arg(value)
                        .query(&mut conn)
                        .unwrap();
                }

                let start = std::time::Instant::now();
                for j in 0..iters {
                    let _: i32 = redis::cmd("DEL")
                        .arg(make_key("del_key", i + j))
                        .query(&mut conn)
                        .unwrap();
                }
                i += iters;
                start.elapsed()
            });
        });

        cleanup_redis(&mut conn);
    }

    group.finish();
}

// EXISTS Benchmarks

fn bench_exists(c: &mut Criterion) {
    let mut group = c.benchmark_group("exists");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let value = random_bytes(256);

    // PostgreSQL UNLOGGED
    if let Some(store) = create_unlogged_store("exists") {
        prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("pg_unlogged/existing", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                i += 1;
                black_box(store.exists(black_box(&key)).unwrap());
            });
        });

        group.bench_function("pg_unlogged/missing", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("missing", i);
                i += 1;
                black_box(store.exists(black_box(&key)).unwrap());
            });
        });
    }

    // PostgreSQL Regular
    if let Some(store) = create_regular_store("exists") {
        prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("pg_regular/existing", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                i += 1;
                black_box(store.exists(black_box(&key)).unwrap());
            });
        });

        group.bench_function("pg_regular/missing", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("missing", i);
                i += 1;
                black_box(store.exists(black_box(&key)).unwrap());
            });
        });
    }

    // Redis
    if let Some(mut conn) = create_redis_connection() {
        let prefix = "bench_exists";
        prepopulate_redis(&mut conn, prefix, NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("redis/existing", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                i += 1;
                let _: bool = redis::cmd("EXISTS")
                    .arg(black_box(&key))
                    .query(&mut conn)
                    .unwrap();
            });
        });

        group.bench_function("redis/missing", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("missing", i);
                i += 1;
                let _: bool = redis::cmd("EXISTS")
                    .arg(black_box(&key))
                    .query(&mut conn)
                    .unwrap();
            });
        });

        cleanup_redis(&mut conn);
    }

    group.finish();
}

// ATOMIC INCREMENT Benchmarks

fn bench_increment(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic/increment");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    // PostgreSQL UNLOGGED
    if let Some(store) = create_unlogged_store("increment") {
        store.set("counter", b"0").unwrap();

        group.bench_function("pg_unlogged", |b| {
            b.iter(|| {
                black_box(store.increment(black_box("counter"), black_box(1)).unwrap());
            });
        });
    }

    // PostgreSQL Regular
    if let Some(store) = create_regular_store("increment") {
        store.set("counter", b"0").unwrap();

        group.bench_function("pg_regular", |b| {
            b.iter(|| {
                black_box(store.increment(black_box("counter"), black_box(1)).unwrap());
            });
        });
    }

    // Redis INCR
    if let Some(mut conn) = create_redis_connection() {
        let _: () = redis::cmd("SET")
            .arg("counter")
            .arg(0)
            .query(&mut conn)
            .unwrap();

        group.bench_function("redis", |b| {
            b.iter(|| {
                let _: i64 = redis::cmd("INCRBY")
                    .arg("counter")
                    .arg(1)
                    .query(&mut conn)
                    .unwrap();
            });
        });

        cleanup_redis(&mut conn);
    }

    group.finish();
}

// TTL SET Benchmarks

fn bench_set_with_ttl(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl/set_ex");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let value = random_bytes(256);
    let ttl = Duration::from_secs(60);

    // PostgreSQL UNLOGGED
    if let Some(store) = create_unlogged_store("set_ttl") {
        group.bench_function("pg_unlogged", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("ttl_key", i);
                i += 1;
                store
                    .set_ex(black_box(&key), black_box(&value), ttl)
                    .unwrap();
            });
        });
    }

    // PostgreSQL Regular
    if let Some(store) = create_regular_store("set_ttl") {
        group.bench_function("pg_regular", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("ttl_key", i);
                i += 1;
                store
                    .set_ex(black_box(&key), black_box(&value), ttl)
                    .unwrap();
            });
        });
    }

    // Redis SETEX
    if let Some(mut conn) = create_redis_connection() {
        group.bench_function("redis", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key("ttl_key", i);
                i += 1;
                let _: () = redis::cmd("SETEX")
                    .arg(black_box(&key))
                    .arg(60)
                    .arg(black_box(&value))
                    .query(&mut conn)
                    .unwrap();
            });
        });

        cleanup_redis(&mut conn);
    }

    group.finish();
}

// SCAN Benchmarks - Prefix Scanning

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let value = random_bytes(256);
    let scan_limits = [10, 50, 100, 500];

    for limit in scan_limits {
        group.throughput(Throughput::Elements(limit as u64));

        // PostgreSQL UNLOGGED
        if let Some(store) = create_unlogged_store(&format!("scan_{}", limit)) {
            for i in 0..NUM_PREPOPULATED_KEYS {
                store.set(&format!("user:{}", i), &value).unwrap();
            }

            group.bench_with_input(
                BenchmarkId::new("pg_unlogged", limit),
                &limit,
                |b, &limit| {
                    use pgkv::ScanOptions;
                    b.iter(|| {
                        let opts = ScanOptions::new().prefix("user:").limit(limit);
                        black_box(store.scan(black_box(opts)).unwrap());
                    });
                },
            );
        }

        // PostgreSQL Regular
        if let Some(store) = create_regular_store(&format!("scan_{}", limit)) {
            for i in 0..NUM_PREPOPULATED_KEYS {
                store.set(&format!("user:{}", i), &value).unwrap();
            }

            group.bench_with_input(
                BenchmarkId::new("pg_regular", limit),
                &limit,
                |b, &limit| {
                    use pgkv::ScanOptions;
                    b.iter(|| {
                        let opts = ScanOptions::new().prefix("user:").limit(limit);
                        black_box(store.scan(black_box(opts)).unwrap());
                    });
                },
            );
        }

        // Redis - SCAN is cursor-based, use KEYS + MGET for fair comparison
        if let Some(mut conn) = create_redis_connection() {
            let prefix = format!("bench_scan_{}", limit);
            for i in 0..NUM_PREPOPULATED_KEYS {
                let _: () = redis::cmd("SET")
                    .arg(format!("{}:user:{}", prefix, i))
                    .arg(&value)
                    .query(&mut conn)
                    .unwrap();
            }

            group.bench_with_input(BenchmarkId::new("redis", limit), &limit, |b, &limit| {
                b.iter(|| {
                    // Redis KEYS + MGET approach
                    let keys: Vec<String> = redis::cmd("KEYS")
                        .arg(format!("{}:user:*", prefix))
                        .query(&mut conn)
                        .unwrap();
                    let limited: Vec<_> = keys.into_iter().take(limit).collect();
                    if !limited.is_empty() {
                        let _: Vec<Option<Vec<u8>>> =
                            redis::cmd("MGET").arg(&limited).query(&mut conn).unwrap();
                    }
                });
            });

            cleanup_redis(&mut conn);
        }
    }

    group.finish();
}

// MIXED WORKLOAD Benchmarks - Realistic Usage Patterns

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(THROUGHPUT_MEASUREMENT_TIME_SECS));

    let value = random_bytes(256);

    // 80% reads, 20% writes - typical web app workload
    // PostgreSQL UNLOGGED
    if let Some(store) = create_unlogged_store("mixed") {
        prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("pg_unlogged/80_read_20_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 5 == 0 {
                    // 20% write
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    store.set(black_box(&key), black_box(&value)).unwrap();
                } else {
                    // 80% read
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    black_box(store.get(black_box(&key)).unwrap());
                }
                i += 1;
            });
        });

        // 50% reads, 50% writes - heavy write workload
        group.bench_function("pg_unlogged/50_read_50_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 2 == 0 {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    store.set(black_box(&key), black_box(&value)).unwrap();
                } else {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    black_box(store.get(black_box(&key)).unwrap());
                }
                i += 1;
            });
        });

        // 95% reads, 5% writes - cache-like workload
        group.bench_function("pg_unlogged/95_read_5_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 20 == 0 {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    store.set(black_box(&key), black_box(&value)).unwrap();
                } else {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    black_box(store.get(black_box(&key)).unwrap());
                }
                i += 1;
            });
        });
    }

    // PostgreSQL Regular
    if let Some(store) = create_regular_store("mixed") {
        prepopulate_store(&store, "key", NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("pg_regular/80_read_20_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 5 == 0 {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    store.set(black_box(&key), black_box(&value)).unwrap();
                } else {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    black_box(store.get(black_box(&key)).unwrap());
                }
                i += 1;
            });
        });

        group.bench_function("pg_regular/50_read_50_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 2 == 0 {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    store.set(black_box(&key), black_box(&value)).unwrap();
                } else {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    black_box(store.get(black_box(&key)).unwrap());
                }
                i += 1;
            });
        });

        group.bench_function("pg_regular/95_read_5_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 20 == 0 {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    store.set(black_box(&key), black_box(&value)).unwrap();
                } else {
                    let key = make_key("key", i % NUM_PREPOPULATED_KEYS);
                    black_box(store.get(black_box(&key)).unwrap());
                }
                i += 1;
            });
        });
    }

    // Redis
    if let Some(mut conn) = create_redis_connection() {
        let prefix = "bench_mixed";
        prepopulate_redis(&mut conn, prefix, NUM_PREPOPULATED_KEYS, &value);

        group.bench_function("redis/80_read_20_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 5 == 0 {
                    let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                    let _: () = redis::cmd("SET")
                        .arg(black_box(&key))
                        .arg(black_box(&value))
                        .query(&mut conn)
                        .unwrap();
                } else {
                    let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                    let _: Option<Vec<u8>> = redis::cmd("GET")
                        .arg(black_box(&key))
                        .query(&mut conn)
                        .unwrap();
                }
                i += 1;
            });
        });

        group.bench_function("redis/50_read_50_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 2 == 0 {
                    let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                    let _: () = redis::cmd("SET")
                        .arg(black_box(&key))
                        .arg(black_box(&value))
                        .query(&mut conn)
                        .unwrap();
                } else {
                    let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                    let _: Option<Vec<u8>> = redis::cmd("GET")
                        .arg(black_box(&key))
                        .query(&mut conn)
                        .unwrap();
                }
                i += 1;
            });
        });

        group.bench_function("redis/95_read_5_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 20 == 0 {
                    let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                    let _: () = redis::cmd("SET")
                        .arg(black_box(&key))
                        .arg(black_box(&value))
                        .query(&mut conn)
                        .unwrap();
                } else {
                    let key = make_key(prefix, i % NUM_PREPOPULATED_KEYS);
                    let _: Option<Vec<u8>> = redis::cmd("GET")
                        .arg(black_box(&key))
                        .query(&mut conn)
                        .unwrap();
                }
                i += 1;
            });
        });

        cleanup_redis(&mut conn);
    }

    group.finish();
}

// THROUGHPUT Benchmarks - Sustained Performance

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.measurement_time(Duration::from_secs(THROUGHPUT_MEASUREMENT_TIME_SECS));
    group.throughput(Throughput::Elements(1));

    let value = random_bytes(256);

    // Sequential writes - measure sustained write throughput
    if let Some(store) = create_unlogged_store("throughput_write") {
        group.bench_function("pg_unlogged/sequential_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                store.set(&make_key("seq", i), &value).unwrap();
                i += 1;
            });
        });
    }

    if let Some(store) = create_regular_store("throughput_write") {
        group.bench_function("pg_regular/sequential_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                store.set(&make_key("seq", i), &value).unwrap();
                i += 1;
            });
        });
    }

    if let Some(mut conn) = create_redis_connection() {
        group.bench_function("redis/sequential_write", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let _: () = redis::cmd("SET")
                    .arg(make_key("seq", i))
                    .arg(&value)
                    .query(&mut conn)
                    .unwrap();
                i += 1;
            });
        });
        cleanup_redis(&mut conn);
    }

    // Overwrite same key - measure update throughput
    if let Some(store) = create_unlogged_store("throughput_update") {
        store.set("hotkey", &value).unwrap();
        group.bench_function("pg_unlogged/update_same_key", |b| {
            b.iter(|| {
                store.set(black_box("hotkey"), black_box(&value)).unwrap();
            });
        });
    }

    if let Some(store) = create_regular_store("throughput_update") {
        store.set("hotkey", &value).unwrap();
        group.bench_function("pg_regular/update_same_key", |b| {
            b.iter(|| {
                store.set(black_box("hotkey"), black_box(&value)).unwrap();
            });
        });
    }

    if let Some(mut conn) = create_redis_connection() {
        let _: () = redis::cmd("SET")
            .arg("hotkey")
            .arg(&value)
            .query(&mut conn)
            .unwrap();
        group.bench_function("redis/update_same_key", |b| {
            b.iter(|| {
                let _: () = redis::cmd("SET")
                    .arg("hotkey")
                    .arg(&value)
                    .query(&mut conn)
                    .unwrap();
            });
        });
        cleanup_redis(&mut conn);
    }

    group.finish();
}

// LARGE VALUE Benchmarks

fn bench_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_values");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    // Test with larger values: 64KB, 256KB, 1MB
    let value_sizes = [64 * 1024, 256 * 1024, 1024 * 1024];

    for size in value_sizes {
        let value = random_bytes(size);
        let size_kb = size / 1024;
        group.throughput(Throughput::Bytes(size as u64));

        // PostgreSQL UNLOGGED
        if let Some(store) = create_unlogged_store(&format!("large_{}", size_kb)) {
            group.bench_with_input(
                BenchmarkId::new("pg_unlogged/set", format!("{}KB", size_kb)),
                &size,
                |b, _| {
                    let mut i = 0u64;
                    b.iter(|| {
                        let key = make_key("large", i);
                        i += 1;
                        store.set(black_box(&key), black_box(&value)).unwrap();
                    });
                },
            );

            // Also test get for large values
            store.set("large_key", &value).unwrap();
            group.bench_with_input(
                BenchmarkId::new("pg_unlogged/get", format!("{}KB", size_kb)),
                &size,
                |b, _| {
                    b.iter(|| {
                        black_box(store.get(black_box("large_key")).unwrap());
                    });
                },
            );
        }

        // PostgreSQL Regular
        if let Some(store) = create_regular_store(&format!("large_{}", size_kb)) {
            group.bench_with_input(
                BenchmarkId::new("pg_regular/set", format!("{}KB", size_kb)),
                &size,
                |b, _| {
                    let mut i = 0u64;
                    b.iter(|| {
                        let key = make_key("large", i);
                        i += 1;
                        store.set(black_box(&key), black_box(&value)).unwrap();
                    });
                },
            );

            store.set("large_key", &value).unwrap();
            group.bench_with_input(
                BenchmarkId::new("pg_regular/get", format!("{}KB", size_kb)),
                &size,
                |b, _| {
                    b.iter(|| {
                        black_box(store.get(black_box("large_key")).unwrap());
                    });
                },
            );
        }

        // Redis
        if let Some(mut conn) = create_redis_connection() {
            group.bench_with_input(
                BenchmarkId::new("redis/set", format!("{}KB", size_kb)),
                &size,
                |b, _| {
                    let mut i = 0u64;
                    b.iter(|| {
                        let key = make_key("large", i);
                        i += 1;
                        let _: () = redis::cmd("SET")
                            .arg(black_box(&key))
                            .arg(black_box(&value))
                            .query(&mut conn)
                            .unwrap();
                    });
                },
            );

            let _: () = redis::cmd("SET")
                .arg("large_key")
                .arg(&value)
                .query(&mut conn)
                .unwrap();
            group.bench_with_input(
                BenchmarkId::new("redis/get", format!("{}KB", size_kb)),
                &size,
                |b, _| {
                    b.iter(|| {
                        let _: Option<Vec<u8>> =
                            redis::cmd("GET").arg("large_key").query(&mut conn).unwrap();
                    });
                },
            );

            cleanup_redis(&mut conn);
        }
    }

    group.finish();
}

// TTL CLEANUP Benchmarks - Test cleanup strategies

fn bench_ttl_cleanup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_cleanup");
    group.measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS));

    let value = random_bytes(256);

    // Test manual cleanup performance
    if let Some(store) = create_unlogged_store("ttl_cleanup") {
        // Create many expired keys
        let past_ttl = Duration::from_nanos(1); // Already expired
        for i in 0..1000 {
            let _ = store.set_ex(&make_key("expired", i), &value, past_ttl);
        }

        // Benchmark cleanup
        group.bench_function("pg_unlogged/cleanup_1000_expired", |b| {
            b.iter(|| {
                black_box(store.cleanup_expired().unwrap());
            });
        });
    }

    group.finish();
}

// Criterion Configuration

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_get_many,
    bench_set_many,
    bench_delete,
    bench_exists,
    bench_increment,
    bench_set_with_ttl,
    bench_scan,
    bench_mixed_workload,
    bench_throughput,
    bench_large_values,
    bench_ttl_cleanup,
);

criterion_main!(benches);
