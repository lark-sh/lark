//! Integration tests for the in-process storage worker.
//!
//! These tests verify the full compaction pipeline:
//! 1. Write enough data to trigger WAL rotation
//! 2. Storage worker picks up the completed WAL file
//! 3. WAL entries are applied to the blob via BlobSession::apply_updates
//! 4. Sequence file is updated
//! 5. Processed WAL files are deleted
//! 6. Data survives restart (reads from updated blob)

mod common;

use common::{TestServer, run_test};
use lark_blob::{ArcValue, BlobSession, StdBlobIO, full_compact, write_blob};
use lark_server::db::{blob_path, read_blob_generation, sidecar_path};
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;

/// Helper: write a blob file at the expected path for a given project/database.
fn write_test_blob(data_dir: &str, project: &str, db_name: &str, tree: &ArcValue) {
    let db_dir = format!("{}/{}/{}", data_dir, project, db_name);
    std::fs::create_dir_all(&db_dir).unwrap();
    let blob_path = format!("{}/blob.lark", db_dir);

    futures::executor::block_on(async {
        let io = StdBlobIO::create(std::path::Path::new(&blob_path)).unwrap();
        write_blob(&io, tree).await.unwrap();
    });
}

/// Generate a large string value of approximately the given size in bytes.
fn large_value(size_bytes: usize) -> String {
    "x".repeat(size_bytes)
}

/// Read the sequence file for a database. Returns 0 if missing.
fn read_sequence(data_dir: &str, project: &str, db_name: &str) -> i64 {
    let path = format!("{}/{}/{}/sequence", data_dir, project, db_name);
    match std::fs::read_to_string(&path) {
        Ok(s) => s.trim().parse().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Count WAL files in the database's wal directory.
fn count_wal_files(data_dir: &str, project: &str, db_name: &str) -> usize {
    let wal_dir = format!("{}/{}/{}/wal", data_dir, project, db_name);
    match std::fs::read_dir(&wal_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "wal")
                    .unwrap_or(false)
            })
            .count(),
        Err(_) => 0,
    }
}

/// Return the blob.lark path for a database directory, if it exists.
fn find_blob_path(data_dir: &str, project: &str, db_name: &str) -> Option<String> {
    let db_dir = format!("{}/{}/{}", data_dir, project, db_name);
    let bp = format!("{}/blob.lark", db_dir);
    if std::path::Path::new(&bp).exists() {
        Some(bp)
    } else {
        None
    }
}

/// Read the blob file and extract the value at a path using StdBlobIO.
fn read_blob_value(
    data_dir: &str,
    project: &str,
    db_name: &str,
    path: &[&str],
) -> Option<serde_json::Value> {
    let blob_path = find_blob_path(data_dir, project, db_name)?;

    futures::executor::block_on(async {
        let io = StdBlobIO::open(std::path::Path::new(&blob_path)).unwrap();
        let session = BlobSession::open(io).await.unwrap();

        match session.read_subtree(path).await {
            Ok(value) => {
                if value.exists() {
                    Some(value.to_value())
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    })
}

/// Get the sidecar file size for a database (0 if doesn't exist).
fn sidecar_size(data_dir: &str, project: &str, db_name: &str) -> u64 {
    let db_dir = format!("{}/{}/{}", data_dir, project, db_name);
    let sp = sidecar_path(std::path::Path::new(&db_dir));
    std::fs::metadata(&sp).map(|m| m.len()).unwrap_or(0)
}

// WAL flush interval is 2 seconds
const WAL_FLUSH_WAIT: Duration = Duration::from_millis(2500);

/// Write bulk data to a connected client to trigger WAL rotation (>5MB),
/// then wait for the storage worker to compact.
async fn trigger_rotation_and_wait(client: &common::TestClient) {
    // Use a small number of keys with large values to avoid filling the blob dictionary.
    // 10 writes × 600KB ≈ 6MB, exceeding the 5MB WAL_MAX_FILE_SIZE.
    let chunk = large_value(600_000);
    for i in 0..10 {
        client
            .set(&format!("/bulk/item_{}", i), json!(&chunk))
            .await
            .unwrap();
    }

    // Wait for WAL flush
    glommio::timer::sleep(WAL_FLUSH_WAIT).await;

    // Give the storage worker time to process compaction.
    for _ in 0..50 {
        glommio::yield_if_needed().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;
    }
}

// =============================================================================
// Basic Compaction Tests
// =============================================================================

/// Write enough data to trigger WAL rotation, then verify the storage worker
/// compacts the WAL into the blob and cleans up.
#[test]
#[ignore] // Slow test — writes 5MB+ to trigger rotation
fn test_storage_worker_compacts_after_rotation() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Write an initial blob with some data
        let tree = ArcValue::from_value(json!({"initial": "data"}));
        write_test_blob(data_dir, "test-project", "compact-db", &tree);

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/compact-db").await;

        // Verify initial data
        assert_eq!(client.once("/initial").await.unwrap(), json!("data"));

        // Trigger WAL rotation and wait for compaction
        trigger_rotation_and_wait(&client).await;

        // Verify: sequence file should be > 0 (compaction happened)
        let seq = read_sequence(data_dir, "test-project", "compact-db");
        assert!(
            seq > 0,
            "Sequence file should be updated after compaction, got {}",
            seq
        );

        // Verify: WAL files are NOT deleted by StorageWorker (that's lark-compact's job).
        // After rotation we expect 2: the completed WAL + the active one.
        let wal_count = count_wal_files(data_dir, "test-project", "compact-db");
        assert!(
            wal_count >= 2,
            "Expected at least 2 WAL files (completed + active), got {}",
            wal_count
        );

        // Verify: sidecar should exist and be non-empty (free list persisted)
        let sidecar_bytes = sidecar_size(data_dir, "test-project", "compact-db");
        assert!(
            sidecar_bytes > 0,
            "Sidecar should exist and contain free list data after compaction, got {} bytes",
            sidecar_bytes
        );

        // Verify: blob should contain the written data
        let chunk = large_value(600_000);
        let blob_val = read_blob_value(data_dir, "test-project", "compact-db", &["bulk", "item_0"]);
        assert_eq!(
            blob_val,
            Some(json!(&chunk)),
            "Blob should contain compacted data"
        );

        // Verify: initial data should also be in the blob
        let initial_val = read_blob_value(data_dir, "test-project", "compact-db", &["initial"]);
        assert_eq!(
            initial_val,
            Some(json!("data")),
            "Initial blob data should survive compaction"
        );

        // Verify: data is still readable through the database
        assert_eq!(client.once("/initial").await.unwrap(), json!("data"));
        assert_eq!(client.once("/bulk/item_0").await.unwrap(), json!(&chunk));

        client.disconnect().await;
    });
}

/// After compaction, restarting the server should load data from the updated blob.
#[test]
#[ignore] // Slow test — writes 5MB+ to trigger rotation
fn test_storage_worker_data_survives_restart() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Start with an initial blob
        let tree = ArcValue::from_value(json!({"original": "from_blob"}));
        write_test_blob(data_dir, "test-project", "restart-compact-db", &tree);

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/restart-compact-db").await;

        // Write a unique marker value
        client
            .set("/marker", json!("compacted_value"))
            .await
            .unwrap();

        // Trigger rotation and wait for compaction
        trigger_rotation_and_wait(&client).await;

        // Verify compaction happened
        let seq = read_sequence(data_dir, "test-project", "restart-compact-db");
        assert!(seq > 0, "Compaction should have happened");

        // Verify sidecar exists before restart
        let sidecar_before = sidecar_size(data_dir, "test-project", "restart-compact-db");
        assert!(sidecar_before > 0, "Sidecar should exist after compaction");

        // Shutdown
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Restart with same data directory
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/restart-compact-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Data that was compacted into the blob should be readable
        let marker = client2.once("/marker").await.unwrap();
        assert_eq!(
            marker,
            json!("compacted_value"),
            "Compacted data should survive restart"
        );

        // Original blob data should also survive
        let original = client2.once("/original").await.unwrap();
        assert_eq!(
            original,
            json!("from_blob"),
            "Original blob data should survive compaction + restart"
        );

        // Bulk data should be readable
        let chunk = large_value(600_000);
        let item = client2.once("/bulk/item_0").await.unwrap();
        assert_eq!(
            item,
            json!(&chunk),
            "Bulk compacted data should survive restart"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

/// Verify that UPDATE operations are correctly compacted (expanded to per-child SETs).
#[test]
#[ignore] // Slow test — writes 5MB+ to trigger rotation
fn test_storage_worker_compacts_updates() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let tree = ArcValue::from_value(json!({
            "profile": {
                "name": "Alice",
                "bio": "Hello",
                "score": 100
            }
        }));
        write_test_blob(data_dir, "test-project", "update-compact-db", &tree);

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/update-compact-db").await;

        // Do an UPDATE (shallow merge)
        client
            .update("/profile", json!({"score": 999, "badge": "gold"}))
            .await
            .unwrap();

        // Trigger rotation and wait for compaction
        trigger_rotation_and_wait(&client).await;

        let seq = read_sequence(data_dir, "test-project", "update-compact-db");
        assert!(seq > 0, "Compaction should have happened");

        // Verify the UPDATE was expanded correctly in the blob
        let score = read_blob_value(
            data_dir,
            "test-project",
            "update-compact-db",
            &["profile", "score"],
        );
        assert_eq!(
            score,
            Some(json!(999)),
            "UPDATE should update score in blob"
        );

        let badge = read_blob_value(
            data_dir,
            "test-project",
            "update-compact-db",
            &["profile", "badge"],
        );
        assert_eq!(
            badge,
            Some(json!("gold")),
            "UPDATE should add badge in blob"
        );

        // Original fields not in the UPDATE should be preserved
        let name = read_blob_value(
            data_dir,
            "test-project",
            "update-compact-db",
            &["profile", "name"],
        );
        assert_eq!(
            name,
            Some(json!("Alice")),
            "Original field should be preserved in blob"
        );

        client.disconnect().await;
    });
}

/// Verify that DELETE operations are correctly compacted.
#[test]
#[ignore] // Slow test — writes 5MB+ to trigger rotation
fn test_storage_worker_compacts_deletes() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let tree = ArcValue::from_value(json!({
            "keep": "yes",
            "remove_me": "goodbye"
        }));
        write_test_blob(data_dir, "test-project", "delete-compact-db", &tree);

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/delete-compact-db").await;

        // Delete a path
        client.remove("/remove_me").await.unwrap();

        // Trigger rotation and wait for compaction
        trigger_rotation_and_wait(&client).await;

        let seq = read_sequence(data_dir, "test-project", "delete-compact-db");
        assert!(seq > 0, "Compaction should have happened");

        // Verify: deleted path should not be in blob
        let removed = read_blob_value(
            data_dir,
            "test-project",
            "delete-compact-db",
            &["remove_me"],
        );
        assert_eq!(
            removed, None,
            "Deleted path should not be in blob after compaction"
        );

        // Verify: kept path should still be in blob
        let kept = read_blob_value(data_dir, "test-project", "delete-compact-db", &["keep"]);
        assert_eq!(
            kept,
            Some(json!("yes")),
            "Non-deleted path should survive compaction"
        );

        client.disconnect().await;
    });
}

/// Verify that a new database (no pre-existing blob) gets a blob created
/// and the storage worker can compact into it.
#[test]
#[ignore] // Slow test — writes 5MB+ to trigger rotation
fn test_storage_worker_compacts_new_database() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // No pre-existing blob — database starts empty
        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/new-compact-db").await;

        // Write a marker value
        client.set("/marker", json!("hello")).await.unwrap();

        // Trigger rotation and wait for compaction
        trigger_rotation_and_wait(&client).await;

        let seq = read_sequence(data_dir, "test-project", "new-compact-db");
        assert!(seq > 0, "Compaction should have happened for new database");

        // Verify blob contains the data
        let marker = read_blob_value(data_dir, "test-project", "new-compact-db", &["marker"]);
        assert_eq!(
            marker,
            Some(json!("hello")),
            "Marker should be in blob after compaction"
        );

        // Verify: sidecar should exist even for a new database (created by StorageWorker)
        let sidecar_bytes = sidecar_size(data_dir, "test-project", "new-compact-db");
        assert!(
            sidecar_bytes > 0,
            "Sidecar should be created for new databases after compaction, got {} bytes",
            sidecar_bytes
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Blob File Rotation Tests
// =============================================================================

/// Check if blob.lark exists and return (generation, "blob.lark") if so.
fn list_blob_files(data_dir: &str, project: &str, db_name: &str) -> Vec<(u64, String)> {
    let db_dir = format!("{}/{}/{}", data_dir, project, db_name);
    let db_path = std::path::Path::new(&db_dir);
    if blob_path(db_path).exists() {
        let generation = read_blob_generation(db_path);
        vec![(generation, "blob.lark".to_string())]
    } else {
        vec![]
    }
}

/// Write 500+ unique field names to trigger a dictionary rebuild,
/// then verify the blob file is updated in-place and data is readable.
#[test]
#[ignore] // Slow test — writes 5MB+ with 510 unique fields to trigger dictionary rebuild
fn test_storage_worker_blob_rotation_dictionary_full() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Start with an initial blob containing a known marker
        let tree = ArcValue::from_value(json!({"original": "marker"}));
        write_test_blob(data_dir, "test-project", "rotation-db", &tree);

        // Verify starting state: blob.lark exists
        let initial_blobs = list_blob_files(data_dir, "test-project", "rotation-db");
        assert_eq!(
            initial_blobs.len(),
            1,
            "Should start with exactly one blob file"
        );
        assert_eq!(initial_blobs[0].0, 0, "Initial blob should be generation 0");

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/rotation-db").await;

        // Write 510 unique field names, each with ~11KB value to exceed 5MB WAL threshold.
        // Dictionary rebuild happens in-place (new dict appended at EOF), no file rotation.
        let padding = large_value(11_000);
        for i in 0..510 {
            client
                .set(&format!("/fields/field_{:04}", i), json!(&padding))
                .await
                .unwrap();
        }

        // Wait for WAL flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Give the storage worker time to process compaction + dictionary rebuild.
        for _ in 0..80 {
            glommio::yield_if_needed().await;
            glommio::timer::sleep(Duration::from_millis(100)).await;
        }

        // Verify: compaction happened
        let seq = read_sequence(data_dir, "test-project", "rotation-db");
        assert!(
            seq > 0,
            "Sequence file should be updated after compaction, got {}",
            seq
        );

        // Verify: blob file stays at blob.lark, generation 0 (dictionary rebuild is in-place, no rotation)
        let blobs = list_blob_files(data_dir, "test-project", "rotation-db");
        assert_eq!(
            blobs.len(),
            1,
            "Should have exactly one blob file, got {:?}",
            blobs
        );
        assert_eq!(
            blobs[0].0, 0,
            "Blob should remain at generation 0 (no rotation)"
        );

        // Verify: original data survives dictionary rebuild
        let original = read_blob_value(data_dir, "test-project", "rotation-db", &["original"]);
        assert_eq!(
            original,
            Some(json!("marker")),
            "Original data should survive dictionary rebuild"
        );

        // Verify: field data from the completed WAL is in the blob.
        // Note: only ~454 of the 510 entries fit in the first WAL file (5MB threshold).
        // The rest are in the active WAL file, not yet compacted.
        let field_0 = read_blob_value(
            data_dir,
            "test-project",
            "rotation-db",
            &["fields", "field_0000"],
        );
        assert_eq!(
            field_0,
            Some(json!(&padding)),
            "Field data should be in blob"
        );

        let field_200 = read_blob_value(
            data_dir,
            "test-project",
            "rotation-db",
            &["fields", "field_0200"],
        );
        assert_eq!(
            field_200,
            Some(json!(&padding)),
            "Mid-range field should be in blob"
        );

        // Verify: data is still readable through the database (blob + WAL overlay)
        assert_eq!(client.once("/original").await.unwrap(), json!("marker"));
        assert_eq!(
            client.once("/fields/field_0000").await.unwrap(),
            json!(&padding)
        );
        // field_0509 may be in the active WAL file — but still readable through the database
        assert_eq!(
            client.once("/fields/field_0509").await.unwrap(),
            json!(&padding)
        );

        client.disconnect().await;
    });
}

/// After a dictionary rebuild, verify that restarting the server loads from the updated blob file.
#[test]
#[ignore] // Slow test — writes 5MB+ to trigger dictionary rebuild
fn test_storage_worker_blob_rotation_survives_restart() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Start with an initial blob
        let tree = ArcValue::from_value(json!({"original": "from_blob"}));
        write_test_blob(data_dir, "test-project", "rotation-restart-db", &tree);

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/rotation-restart-db").await;

        // Write 510 unique fields with padding to trigger WAL rotation + dictionary rebuild
        let padding = large_value(11_000);
        for i in 0..510 {
            client
                .set(&format!("/fields/field_{:04}", i), json!(&padding))
                .await
                .unwrap();
        }

        // Wait for compaction + dictionary rebuild
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;
        for _ in 0..80 {
            glommio::yield_if_needed().await;
            glommio::timer::sleep(Duration::from_millis(100)).await;
        }

        // Verify blob stays at blob.lark, generation 0 (dictionary rebuild is in-place, no rotation)
        let blobs = list_blob_files(data_dir, "test-project", "rotation-restart-db");
        assert_eq!(blobs.len(), 1, "Should have one blob file");
        assert_eq!(
            blobs[0].0, 0,
            "Blob should remain at generation 0 (no rotation)"
        );

        // Shutdown
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Restart with same data directory
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/rotation-restart-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Original data should be readable after dictionary rebuild + restart
        let original = client2.once("/original").await.unwrap();
        assert_eq!(
            original,
            json!("from_blob"),
            "Original data should survive dictionary rebuild + restart"
        );

        // Field data should be readable
        let field_0 = client2.once("/fields/field_0000").await.unwrap();
        assert_eq!(
            field_0,
            json!(&padding),
            "Field data should survive dictionary rebuild + restart"
        );

        let field_509 = client2.once("/fields/field_0509").await.unwrap();
        assert_eq!(
            field_509,
            json!(&padding),
            "Last field should survive dictionary rebuild + restart"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// External Full Compaction (Blob Rotation) Tests
// =============================================================================

/// Simulate an external full compaction (lark-compact) replacing blob.lark
/// while the server is running. The StorageWorker detects the generation change
/// on its next compaction attempt, drops cached state, re-opens blob.lark,
/// and notifies the Database via compaction_complete with the new blob_generation.
#[test]
#[ignore] // Slow test — writes 5MB+ across multiple WAL rotations
fn test_external_full_compaction_blob_rotation() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Write an initial blob with some data
        let tree = ArcValue::from_value(json!({"initial": "data", "keep": "this"}));
        write_test_blob(data_dir, "test-project", "rotation-db", &tree);

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/rotation-db").await;

        // Verify initial data is readable
        assert_eq!(client.once("/initial").await.unwrap(), json!("data"));
        assert_eq!(client.once("/keep").await.unwrap(), json!("this"));

        // Write some data via the client so it goes through WAL
        client.set("/written", json!("via_client")).await.unwrap();

        // Trigger WAL rotation so the data is compacted into blob.lark
        trigger_rotation_and_wait(&client).await;

        // Verify blob.lark exists and has the data
        let blobs = list_blob_files(data_dir, "test-project", "rotation-db");
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].0, 0, "Should be generation 0");

        // Verify data is still readable
        assert_eq!(client.once("/initial").await.unwrap(), json!("data"));
        assert_eq!(client.once("/written").await.unwrap(), json!("via_client"));

        // --- Simulate external full compaction (what lark-compact does) ---
        // Uses the rename dance: compact to tmp, delete sidecar, rename blob.lark → blob.old.lark,
        // rename tmp → blob.lark, bump blob.generation, delete blob.old.lark.
        let db_dir = format!("{}/test-project/rotation-db", data_dir);
        let db_path = std::path::Path::new(&db_dir);
        let bp = blob_path(db_path);
        let blob_tmp = db_path.join("blob.lark.tmp");
        let blob_old = db_path.join("blob.old.lark");

        // Run full_compact: blob.lark -> blob.lark.tmp
        {
            let src = StdBlobIO::open(&bp).unwrap();
            let dst = StdBlobIO::create(&blob_tmp).unwrap();
            full_compact(&src, &dst).await.unwrap();
            lark_blob::BlobIO::sync(&dst).await.unwrap();
        }

        // Rename dance
        let _ = std::fs::remove_file(sidecar_path(db_path));
        std::fs::rename(&bp, &blob_old).unwrap();
        std::fs::rename(&blob_tmp, &bp).unwrap();
        std::fs::write(db_path.join("blob.generation"), "1").unwrap();
        let _ = std::fs::remove_file(&blob_old);

        // Verify blob.lark still exists on disk with generation 1
        let blobs = list_blob_files(data_dir, "test-project", "rotation-db");
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].0, 1, "Should now be generation 1");

        // Reads still work — tree is authoritative (data already in memory)
        assert_eq!(client.once("/initial").await.unwrap(), json!("data"));
        assert_eq!(client.once("/keep").await.unwrap(), json!("this"));
        assert_eq!(client.once("/written").await.unwrap(), json!("via_client"));

        // Writes work — WAL is unaffected by blob rotation
        client.set("/after_rotation", json!("works")).await.unwrap();
        assert_eq!(
            client.once("/after_rotation").await.unwrap(),
            json!("works")
        );

        // StorageWorker detects generation change on next compaction, drops cached
        // state and re-opens blob.lark. Sends compaction_complete with blob_generation=1.
        // The Database switches its BlobSession/CachedIO.
        trigger_rotation_and_wait(&client).await;

        // Data should survive compaction
        assert_eq!(client.once("/initial").await.unwrap(), json!("data"));
        assert_eq!(
            client.once("/after_rotation").await.unwrap(),
            json!("works")
        );

        // Verify blob is still at generation 1
        let blobs = list_blob_files(data_dir, "test-project", "rotation-db");
        assert_eq!(blobs.len(), 1);
        assert_eq!(
            blobs[0].0, 1,
            "Should still be generation 1 after storage worker compaction"
        );

        client.disconnect().await;
        server.shutdown().await;
    });
}
