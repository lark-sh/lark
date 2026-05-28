//! Array storage and read-coercion behavior.
//!
//! Arrays are stored as integer-keyed maps; there is no distinct array type on
//! disk. On read, a node renders as a JSON array when it is non-empty, every key
//! is a canonical non-negative integer, and `maxKey < 2 * numKeys`; otherwise it
//! renders as an object. Absent indices in `[0, maxKey]` read back as `null`.
//! A `null` write deletes the target, so stored data never contains nulls.

mod common;

use common::{TestServer, run_test};
use serde_json::json;

// =============================================================================
// Round-trip
// =============================================================================

#[test]
fn test_dense_array_round_trips() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client.set("/arr", json!(["a", "b", "c"])).await.unwrap();
        assert_eq!(client.once("/arr").await.unwrap(), json!(["a", "b", "c"]));
    });
}

#[test]
fn test_array_of_objects_round_trips() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        let v = json!([{ "a": 1, "b": 2 }, { "a": 3, "b": 4 }]);
        client.set("/items", v.clone()).await.unwrap();
        assert_eq!(client.once("/items").await.unwrap(), v);
    });
}

// =============================================================================
// Partial writes preserve siblings
// =============================================================================

#[test]
fn test_partial_write_into_array_preserves_siblings() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client
            .set(
                "/items",
                json!([{ "a": 1, "b": 2 }, { "a": 3, "b": 4 }, { "a": 5, "b": 6 }]),
            )
            .await
            .unwrap();

        // Modify one field of one element.
        client.set("/items/0/b", json!(99)).await.unwrap();

        assert_eq!(
            client.once("/items").await.unwrap(),
            json!([{ "a": 1, "b": 99 }, { "a": 3, "b": 4 }, { "a": 5, "b": 6 }])
        );
    });
}

#[test]
fn test_set_single_index_preserves_other_elements() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client
            .set("/arr", json!(["a", "b", "c", "d"]))
            .await
            .unwrap();
        client.set("/arr/2", json!("Z")).await.unwrap();

        assert_eq!(
            client.once("/arr").await.unwrap(),
            json!(["a", "b", "Z", "d"])
        );
    });
}

#[test]
fn test_update_multiple_indices_preserves_rest() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client
            .set("/arr", json!(["a", "b", "c", "d", "e"]))
            .await
            .unwrap();
        client
            .update("/arr", json!({ "1": "B", "3": "D" }))
            .await
            .unwrap();

        assert_eq!(
            client.once("/arr").await.unwrap(),
            json!(["a", "B", "c", "D", "e"])
        );
    });
}

// =============================================================================
// Read coercion: integer-keyed maps render as arrays
// =============================================================================

#[test]
fn test_integer_keyed_object_reads_as_array() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client
            .set("/x", json!({ "0": "a", "1": "b", "2": "c" }))
            .await
            .unwrap();
        assert_eq!(client.once("/x").await.unwrap(), json!(["a", "b", "c"]));
    });
}

#[test]
fn test_gaps_read_back_as_null() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        // {0,2}: maxKey 2 < 2*2 -> array, gap at 1 is null.
        client
            .set("/a", json!({ "0": "x", "2": "z" }))
            .await
            .unwrap();
        assert_eq!(client.once("/a").await.unwrap(), json!(["x", null, "z"]));

        // {1}: maxKey 1 < 2*1 -> array, leading gap at 0 is null.
        client.set("/b", json!({ "1": "y" })).await.unwrap();
        assert_eq!(client.once("/b").await.unwrap(), json!([null, "y"]));
    });
}

/// The exact threshold: render as an array iff `maxKey < 2 * numKeys`.
#[test]
fn test_coercion_threshold() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        // n=2, max=3 -> 3 < 4 -> array
        client
            .set("/t1", json!({ "0": "a", "3": "d" }))
            .await
            .unwrap();
        assert_eq!(
            client.once("/t1").await.unwrap(),
            json!(["a", null, null, "d"])
        );

        // n=2, max=4 -> 4 < 4 false -> object
        client
            .set("/t2", json!({ "0": "a", "4": "e" }))
            .await
            .unwrap();
        assert_eq!(
            client.once("/t2").await.unwrap(),
            json!({ "0": "a", "4": "e" })
        );

        // n=3, max=5 -> 5 < 6 -> array
        client
            .set("/t3", json!({ "0": "a", "1": "b", "5": "f" }))
            .await
            .unwrap();
        assert_eq!(
            client.once("/t3").await.unwrap(),
            json!(["a", "b", null, null, null, "f"])
        );

        // n=3, max=6 -> 6 < 6 false -> object
        client
            .set("/t4", json!({ "0": "a", "1": "b", "6": "g" }))
            .await
            .unwrap();
        assert_eq!(
            client.once("/t4").await.unwrap(),
            json!({ "0": "a", "1": "b", "6": "g" })
        );
    });
}

// =============================================================================
// Non-canonical keys stay objects
// =============================================================================

#[test]
fn test_noncanonical_integer_keys_stay_object() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        // Leading zeros are not canonical integers.
        let lead = json!({ "01": "a", "02": "b" });
        client.set("/lead", lead.clone()).await.unwrap();
        assert_eq!(client.once("/lead").await.unwrap(), lead);

        // Negative key.
        let neg = json!({ "-1": "a", "0": "b" });
        client.set("/neg", neg.clone()).await.unwrap();
        assert_eq!(client.once("/neg").await.unwrap(), neg);

        // Mixed integer and string keys.
        let mixed = json!({ "0": "a", "x": "b" });
        client.set("/mixed", mixed.clone()).await.unwrap();
        assert_eq!(client.once("/mixed").await.unwrap(), mixed);
    });
}

// =============================================================================
// Deletes and sparsity
// =============================================================================

#[test]
fn test_null_element_write_deletes_it() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client.set("/arr", json!(["a", "b", "c"])).await.unwrap();
        // Deleting index 1 leaves {0,2}: max 2 < 2*2 -> array with null gap.
        client.set("/arr/1", json!(null)).await.unwrap();
        assert_eq!(client.once("/arr").await.unwrap(), json!(["a", null, "c"]));
    });
}

#[test]
fn test_delete_until_sparse_becomes_object() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client
            .set("/arr", json!(["a", "b", "c", "d", "e"]))
            .await
            .unwrap();

        // Remove indices 0, 1, 3 -> {2,4}: max 4 < 2*2 false -> object.
        client.remove("/arr/0").await.unwrap();
        client.remove("/arr/1").await.unwrap();
        client.remove("/arr/3").await.unwrap();

        assert_eq!(
            client.once("/arr").await.unwrap(),
            json!({ "2": "c", "4": "e" })
        );
    });
}

#[test]
fn test_empty_array_deletes_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client.set("/arr", json!(["a", "b"])).await.unwrap();
        client.set("/arr", json!([])).await.unwrap();
        assert_eq!(client.once("/arr").await.unwrap(), json!(null));
    });
}

// =============================================================================
// Nesting
// =============================================================================

#[test]
fn test_nested_array_under_object_partial_write() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();
        client.connect("db").await;

        client
            .set("/doc", json!({ "tags": ["x", "y", "z"], "name": "n" }))
            .await
            .unwrap();
        client.set("/doc/tags/1", json!("Y")).await.unwrap();

        assert_eq!(
            client.once("/doc").await.unwrap(),
            json!({ "tags": ["x", "Y", "z"], "name": "n" })
        );
    });
}
