//! Benchmarks: write throughput, navigation latency, incremental compaction, full re-compaction.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use lark_blob::arc_value::ArcValue;
use lark_blob::compact::full_compact;
use lark_blob::incremental::apply_updates;
use lark_blob::io::{BlobIO, MemBlobIO};
use lark_blob::session::BlobSession;
use lark_blob::session_reader::navigate_raw;
use lark_blob::test_helpers::generate_game_database;
use lark_blob::writer::write_blob;

/// Benchmark: ArcValue -> blob serialization throughput.
fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    for &(games, chars, pages, msgs, label) in &[
        (10, 20, 3, 50, "small_10g"),
        (50, 20, 3, 50, "medium_50g"),
        (200, 20, 3, 50, "large_200g"),
    ] {
        let tree = generate_game_database(games, chars, pages, msgs);

        group.bench_with_input(BenchmarkId::new("write", label), &tree, |b, tree| {
            b.iter(|| {
                let io = MemBlobIO::new();
                block_on(write_blob(&io, tree)).unwrap();
            });
        });
    }
    group.finish();
}

/// Benchmark: navigation to a leaf path.
fn bench_navigation(c: &mut Criterion) {
    let mut group = c.benchmark_group("navigation");

    // Build a reasonably large blob
    let tree = generate_game_database(100, 20, 3, 50);
    let io = MemBlobIO::new();
    block_on(write_blob(&io, &tree)).unwrap();
    let session = block_on(BlobSession::open(io)).unwrap();
    let header = session.header().clone();
    let dict = session.dict().clone();

    // Navigate to a deep leaf: games/game_50/characters/char_10/hp (depth=5)
    group.bench_function("leaf_depth_5", |b| {
        b.iter(|| {
            block_on(session.read_subtree(&["games", "game_50", "characters", "char_10", "hp"]))
                .unwrap();
        });
    });

    // Navigate to a subtree: games/game_50/characters (depth=3)
    group.bench_function("subtree_depth_3_navigate_only", |b| {
        b.iter(|| {
            block_on(navigate_raw(
                session.io(),
                &header,
                &dict,
                &["games", "game_50", "characters"],
            ))
            .unwrap();
        });
    });

    // Navigate to root child: games (depth=1)
    group.bench_function("root_child_depth_1", |b| {
        b.iter(|| {
            block_on(navigate_raw(session.io(), &header, &dict, &["games"])).unwrap();
        });
    });

    group.finish();
}

/// Benchmark: subtree read (navigation + deserialization).
fn bench_subtree_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("subtree_read");

    let tree = generate_game_database(100, 20, 3, 50);
    let io = MemBlobIO::new();
    block_on(write_blob(&io, &tree)).unwrap();
    let session = block_on(BlobSession::open(io)).unwrap();

    // Read a single character (small subtree)
    group.bench_function("single_character", |b| {
        b.iter(|| {
            block_on(session.read_subtree(&["games", "game_50", "characters", "char_10"])).unwrap();
        });
    });

    // Read all characters in a game (medium subtree)
    group.bench_function("all_characters_in_game", |b| {
        b.iter(|| {
            block_on(session.read_subtree(&["games", "game_50", "characters"])).unwrap();
        });
    });

    // Read an entire game (large subtree)
    group.bench_function("entire_game", |b| {
        b.iter(|| {
            block_on(session.read_subtree(&["games", "game_50"])).unwrap();
        });
    });

    // Read entire blob
    group.bench_function("entire_blob", |b| {
        b.iter(|| {
            block_on(session.read_subtree(&[])).unwrap();
        });
    });

    group.finish();
}

/// Benchmark: incremental compaction (apply_updates).
fn bench_incremental(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_compaction");

    let tree = generate_game_database(50, 20, 3, 50);
    let base_io = MemBlobIO::new();
    block_on(write_blob(&base_io, &tree)).unwrap();
    let base_data = base_io.data().to_vec();

    // Single scalar update
    group.bench_function("single_scalar_update", |b| {
        b.iter_batched(
            || {
                let io = MemBlobIO::new();
                block_on(io.append(&base_data)).unwrap();
                io
            },
            |io| {
                let updates = vec![(
                    vec![
                        "games".to_string(),
                        "game_25".to_string(),
                        "characters".to_string(),
                        "char_10".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(42i64)),
                )];
                block_on(apply_updates(&io, &updates)).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // 100 scalar updates (simulating a batch of WAL entries)
    group.bench_function("100_scalar_updates", |b| {
        b.iter_batched(
            || {
                let io = MemBlobIO::new();
                block_on(io.append(&base_data)).unwrap();
                io
            },
            |io| {
                let updates: Vec<_> = (0..100)
                    .map(|i| {
                        (
                            vec![
                                "games".to_string(),
                                format!("game_{}", i % 50),
                                "characters".to_string(),
                                format!("char_{}", i % 20),
                                "hp".to_string(),
                            ],
                            Some(ArcValue::from(42i64)),
                        )
                    })
                    .collect();
                block_on(apply_updates(&io, &updates)).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark: full re-compaction.
fn bench_full_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_compaction");
    group.sample_size(20);

    let tree = generate_game_database(50, 20, 3, 50);
    let src_io = MemBlobIO::new();
    block_on(write_blob(&src_io, &tree)).unwrap();

    // Full compact a clean blob
    group.bench_function("clean_50_games", |b| {
        b.iter(|| {
            let dst = MemBlobIO::new();
            block_on(full_compact(&src_io, &dst)).unwrap();
        });
    });

    // Full compact a dirty blob (with updates applied)
    let dirty_io = MemBlobIO::new();
    block_on(dirty_io.append(&src_io.data())).unwrap();
    let updates: Vec<_> = (0..200)
        .map(|i| {
            (
                vec![
                    "games".to_string(),
                    format!("game_{}", i % 50),
                    "characters".to_string(),
                    format!("char_{}", i % 20),
                    "name".to_string(),
                ],
                Some(ArcValue::from(format!(
                    "Updated character name that is longer than original for game {} char {}",
                    i % 50,
                    i % 20,
                ))),
            )
        })
        .collect();
    block_on(apply_updates(&dirty_io, &updates)).unwrap();

    group.bench_function("dirty_50_games_200_updates", |b| {
        b.iter(|| {
            let dst = MemBlobIO::new();
            block_on(full_compact(&dirty_io, &dst)).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_navigation,
    bench_subtree_read,
    bench_incremental,
    bench_full_compact,
);
criterion_main!(benches);
