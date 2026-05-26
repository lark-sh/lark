//! Benchmark: simulated Lark workload (promotion, updates, compaction).
//!
//! Simulates a multiplayer-game workload:
//! - Promote subtrees (game load → read from blob)
//! - Apply writes (gameplay → incremental compaction)
//! - Full re-compaction cycle

use criterion::{Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use lark_blob::arc_value::ArcValue;
use lark_blob::compact::full_compact;
use lark_blob::incremental::apply_updates;
use lark_blob::io::{BlobIO, MemBlobIO};
use lark_blob::session::BlobSession;
use lark_blob::test_helpers::generate_game_database;
use lark_blob::writer::write_blob;

/// Simulated Lark workload benchmark.
fn bench_lark_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("lark_workload");
    group.sample_size(10);

    // Generate a database: 200 games, each with 20 chars, 3 pages, 50 chat msgs
    let tree = generate_game_database(200, 20, 3, 50);
    let io = MemBlobIO::new();
    let stats = block_on(write_blob(&io, &tree)).unwrap();
    let blob_data = io.data().to_vec();

    eprintln!(
        "Blob size: {:.2} MB, nodes: {}, dict fields: {}",
        stats.total_size as f64 / 1_048_576.0,
        stats.node_count,
        stats.dict_field_count
    );

    // Benchmark: promote 50 game subtrees (simulate loading 50 games)
    group.bench_function("promote_50_games", |b| {
        let session =
            block_on(BlobSession::open(block_on(io.clone_for_reading()).unwrap())).unwrap();

        b.iter(|| {
            block_on(async {
                for g in 0..50 {
                    let game_id = format!("game_{}", g);
                    session.read_subtree(&["games", &game_id]).await.unwrap();
                }
            });
        });
    });

    // Benchmark: promote 50 individual characters (smaller subtrees)
    group.bench_function("promote_50_characters", |b| {
        let session =
            block_on(BlobSession::open(block_on(io.clone_for_reading()).unwrap())).unwrap();

        b.iter(|| {
            block_on(async {
                for i in 0..50 {
                    let game_id = format!("game_{}", i % 200);
                    let char_id = format!("char_{}", i % 20);
                    session
                        .read_subtree(&["games", &game_id, "characters", &char_id])
                        .await
                        .unwrap();
                }
            });
        });
    });

    // Benchmark: apply 1000 scalar writes then promote
    group.bench_function("1000_writes_then_promote", |b| {
        b.iter_batched(
            || {
                let io = MemBlobIO::new();
                block_on(io.append(&blob_data)).unwrap();
                io
            },
            |io| {
                block_on(async {
                    // Apply 1000 writes (hp updates across games)
                    let updates: Vec<_> = (0..1000)
                        .map(|i| {
                            (
                                vec![
                                    "games".to_string(),
                                    format!("game_{}", i % 200),
                                    "characters".to_string(),
                                    format!("char_{}", i % 20),
                                    "hp".to_string(),
                                ],
                                Some(ArcValue::from(i as i64)),
                            )
                        })
                        .collect();
                    apply_updates(&io, &updates).await.unwrap();

                    // Then promote 20 games
                    let session = BlobSession::open(io.clone_for_reading().await.unwrap())
                        .await
                        .unwrap();
                    for g in 0..20 {
                        let game_id = format!("game_{}", g);
                        session.read_subtree(&["games", &game_id]).await.unwrap();
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });

    // Benchmark: full cycle (write + updates + compact + read-back)
    group.bench_function("full_cycle_compact_and_verify", |b| {
        b.iter_batched(
            || {
                let io = MemBlobIO::new();
                block_on(io.append(&blob_data)).unwrap();
                io
            },
            |io| {
                block_on(async {
                    // Apply updates
                    let updates: Vec<_> = (0..200)
                        .map(|i| {
                            (
                                vec![
                                    "games".to_string(),
                                    format!("game_{}", i % 200),
                                    "characters".to_string(),
                                    format!("char_{}", i % 20),
                                    "name".to_string(),
                                ],
                                Some(ArcValue::from(format!("Updated name {}", i))),
                            )
                        })
                        .collect();
                    apply_updates(&io, &updates).await.unwrap();

                    // Full compact
                    let dst = MemBlobIO::new();
                    full_compact(&io, &dst).await.unwrap();

                    // Read back to verify
                    let session = BlobSession::open(dst).await.unwrap();
                    session.read_subtree(&["games", "game_0"]).await.unwrap();
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// Benchmark: StdBlobIO with real file I/O (if temp dir is on SSD).
fn bench_file_io(c: &mut Criterion) {
    use lark_blob::io::StdBlobIO;

    let mut group = c.benchmark_group("file_io");
    group.sample_size(10);

    let tree = generate_game_database(50, 20, 3, 50);
    let mem_io = MemBlobIO::new();
    block_on(write_blob(&mem_io, &tree)).unwrap();

    let dir = std::env::temp_dir().join("larkblob_bench");
    std::fs::create_dir_all(&dir).unwrap();
    let blob_path = dir.join("bench.blob");

    // Write to file
    {
        let file_io = StdBlobIO::create(&blob_path).unwrap();
        block_on(write_blob(&file_io, &tree)).unwrap();
    }

    // Benchmark: navigate + read from file
    group.bench_function("file_navigate_leaf", |b| {
        let io = StdBlobIO::open(&blob_path).unwrap();
        let session = block_on(BlobSession::open(io)).unwrap();

        b.iter(|| {
            block_on(session.read_subtree(&["games", "game_25", "characters", "char_10", "hp"]))
                .unwrap();
        });
    });

    // Benchmark: read subtree from file
    group.bench_function("file_read_game_subtree", |b| {
        let io = StdBlobIO::open(&blob_path).unwrap();
        let session = block_on(BlobSession::open(io)).unwrap();

        b.iter(|| {
            block_on(session.read_subtree(&["games", "game_25"])).unwrap();
        });
    });

    std::fs::remove_file(&blob_path).ok();
    group.finish();
}

criterion_group!(benches, bench_lark_workload, bench_file_io);
criterion_main!(benches);
