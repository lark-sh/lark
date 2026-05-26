//! Rules and Authorization integration tests.
//!
//! Tests for security rules (.read, .write) and authentication.

mod common;

use common::{TestServer, run_test};
use serde_json::json;

// =============================================================================
// Basic Rules Tests
// =============================================================================

#[test]
fn test_rules_block_unauthorized_write() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that deny all writes
        server
            .set_rules(
                "rules-test-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": false
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("rules-test-db").await;

        // Try to write - should fail
        let result = client.set("/players/abc", "data").await;
        assert!(
            result.is_err(),
            "Expected write to fail due to rules, but it succeeded"
        );
    });
}

#[test]
fn test_rules_allow_authorized_write() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that allow all writes
        server
            .set_rules(
                "rules-allow-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("rules-allow-db").await;

        // Write should succeed
        client
            .set("/players/abc", "data")
            .await
            .expect("Expected write to succeed");

        // Verify the write worked
        let value = client.once("/players/abc").await.expect("Failed to read");
        assert_eq!(value, json!("data"));
    });
}

#[test]
fn test_rules_block_unauthorized_read() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that deny all reads but allow writes
        server
            .set_rules(
                "rules-read-db",
                json!({
                    "rules": {
                        ".read": false,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("rules-read-db").await;

        // Write should succeed
        client
            .set("/players/abc", "data")
            .await
            .expect("Write failed unexpectedly");

        // Read should fail
        let result = client.once("/players/abc").await;
        assert!(
            result.is_err(),
            "Expected read to fail due to rules, but it succeeded"
        );
    });
}

// =============================================================================
// Path-specific Rules Tests
// =============================================================================

#[test]
fn test_rules_path_specific() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules where only /public is readable/writable
        server
            .set_rules(
                "path-rules-db",
                json!({
                    "rules": {
                        ".read": false,
                        ".write": false,
                        "public": {
                            ".read": true,
                            ".write": true
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("path-rules-db").await;

        // Write to /public should succeed
        client
            .set("/public/data", "allowed")
            .await
            .expect("Write to /public should succeed");

        // Read from /public should succeed
        let value = client
            .once("/public/data")
            .await
            .expect("Read from /public should succeed");
        assert_eq!(value, json!("allowed"));

        // Write to /private should fail
        let result = client.set("/private/data", "denied").await;
        assert!(result.is_err(), "Write to /private should fail");

        // Read from /private should fail
        let result = client.once("/private/data").await;
        assert!(result.is_err(), "Read from /private should fail");
    });
}

#[test]
fn test_rules_wildcard_path() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules with wildcard
        server
            .set_rules(
                "wildcard-rules-db",
                json!({
                    "rules": {
                        "users": {
                            "$uid": {
                                ".read": true,
                                ".write": true
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("wildcard-rules-db").await;

        // Write to any user path should work
        client
            .set("/users/alice/name", "Alice")
            .await
            .expect("Write to /users/alice should succeed");

        client
            .set("/users/bob/name", "Bob")
            .await
            .expect("Write to /users/bob should succeed");

        // Read should work
        let alice = client
            .once("/users/alice/name")
            .await
            .expect("Read should succeed");
        assert_eq!(alice, json!("Alice"));
    });
}

// =============================================================================
// Auth-based Rules Tests
// =============================================================================

#[test]
fn test_rules_auth_uid_check() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that only allow users to write to their own path
        server
            .set_rules(
                "auth-rules-db",
                json!({
                    "rules": {
                        "users": {
                            "$uid": {
                                ".read": true,
                                ".write": "auth != null && auth.uid == $uid"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        // Connect as user "alice"
        let mut client = server.client();
        client.connect_as_user("auth-rules-db", "alice").await;

        // Alice should be able to write to her own path
        client
            .set("/users/alice/profile", json!({"name": "Alice"}))
            .await
            .expect("Alice should write to her own path");

        // Alice should NOT be able to write to bob's path
        let result = client
            .set("/users/bob/profile", json!({"name": "Hacked"}))
            .await;
        assert!(
            result.is_err(),
            "Alice should not be able to write to bob's path"
        );
    });
}

#[test]
fn test_rules_anonymous_denied() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that require auth
        server
            .set_rules(
                "auth-required-db",
                json!({
                    "rules": {
                        ".read": "auth != null",
                        ".write": "auth != null"
                    }
                }),
            )
            .expect("Failed to set rules");

        // Connect anonymously (no auth)
        let mut client = server.client();
        client.connect("auth-required-db").await;

        // Anonymous user should not be able to write
        let result = client.set("/data", "value").await;
        assert!(result.is_err(), "Anonymous write should fail");

        // Anonymous user should not be able to read
        let result = client.once("/data").await;
        assert!(result.is_err(), "Anonymous read should fail");
    });
}

#[test]
fn test_rules_authenticated_allowed() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that require auth
        server
            .set_rules(
                "auth-allowed-db",
                json!({
                    "rules": {
                        ".read": "auth != null",
                        ".write": "auth != null"
                    }
                }),
            )
            .expect("Failed to set rules");

        // Connect as authenticated user
        let mut client = server.client();
        client.connect_as_user("auth-allowed-db", "user123").await;

        // Authenticated user should be able to write
        client
            .set("/data", "value")
            .await
            .expect("Authenticated write should succeed");

        // Authenticated user should be able to read
        let value = client
            .once("/data")
            .await
            .expect("Authenticated read should succeed");
        assert_eq!(value, json!("value"));
    });
}

// =============================================================================
// Subscribe with Rules Tests
// =============================================================================

#[test]
fn test_rules_block_subscribe() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules that deny reads
        server
            .set_rules(
                "sub-rules-db",
                json!({
                    "rules": {
                        ".read": false,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("sub-rules-db").await;

        // Subscribe should fail due to read permission
        let result = client.subscribe("/data", &["value"]).await;
        assert!(result.is_err(), "Subscribe should fail when read is denied");
    });
}

// =============================================================================
// Update and Remove with Rules
// =============================================================================

#[test]
fn test_rules_block_update() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "update-rules-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": false
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("update-rules-db").await;

        // Update should fail
        let result = client.update("/data", json!({"key": "value"})).await;
        assert!(result.is_err(), "Update should fail when write is denied");
    });
}

#[test]
fn test_rules_block_remove() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "remove-rules-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": false
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("remove-rules-db").await;

        // Remove should fail
        let result = client.remove("/data").await;
        assert!(result.is_err(), "Remove should fail when write is denied");
    });
}

// =============================================================================
// No Rules (Default Allow)
// =============================================================================

#[test]
fn test_no_rules_allows_all() {
    run_test(|| async {
        let server = TestServer::new();
        // Don't set any rules

        let mut client = server.client();
        client.connect("no-rules-db").await;

        // Write should succeed
        client
            .set("/data", "value")
            .await
            .expect("Write should succeed with no rules");

        // Read should succeed
        let value = client
            .once("/data")
            .await
            .expect("Read should succeed with no rules");
        assert_eq!(value, json!("value"));
    });
}

// =============================================================================
// UPDATE Operation Rules Tests
// =============================================================================

/// Test that UPDATE operations check rules at the update path level first.
///
/// This tests the scenario where:
/// - A rule at the parent level (e.g., $pathId) grants access based on newData at that level
/// - The update includes multiple properties
/// - The parent rule should grant access for the entire update
///
/// Example: Creating a new "path" object with layer='objects' should be allowed
/// by the $pathId rule, even though there's no rule granting individual property writes.
#[test]
fn test_update_parent_rule_grants_access_for_new_object() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules similar to example paths structure:
        // - Parent level allows creating new objects if layer='objects'
        // - Property level only allows specific properties (not all)
        server
            .set_rules(
                "update-parent-rule-db",
                json!({
                    "rules": {
                        ".read": true,
                        "paths": {
                            "$pageId": {
                                "$pathId": {
                                    // Allow creating new paths if layer='objects'
                                    ".write": "!data.exists() && newData.child('layer').val() == 'objects'",
                                    "$property": {
                                        // Only allow editing x and y properties
                                        ".write": "$property === 'x' || $property === 'y'"
                                    }
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("update-parent-rule-db").await;

        // UPDATE a new path object with multiple properties including 'layer'
        // The $pathId rule should grant access because:
        // - !data.exists() is true (new object)
        // - newData.child('layer').val() == 'objects' is true
        let result = client
            .update(
                "/paths/page-123/path-456",
                json!({
                    "layer": "objects",
                    "stroke": "#FF0000",
                    "controlledby": "player-1",
                    "x": 100,
                    "y": 200
                }),
            )
            .await;

        assert!(
            result.is_ok(),
            "UPDATE should succeed - parent rule grants access for new object with layer='objects'. Error: {:?}",
            result.err()
        );

        // Verify the data was written
        let value = client
            .once("/paths/page-123/path-456")
            .await
            .expect("Should be able to read the path");
        assert_eq!(value["layer"], json!("objects"));
        assert_eq!(value["stroke"], json!("#FF0000"));
    });
}

/// Test that UPDATE to existing object is denied when parent rule doesn't allow it.
#[test]
fn test_update_parent_rule_denies_modification_of_existing() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "update-deny-existing-db",
                json!({
                    "rules": {
                        ".read": true,
                        "paths": {
                            "$pageId": {
                                "$pathId": {
                                    // Only allow creating NEW objects with layer='objects'
                                    ".write": "!data.exists() && newData.child('layer').val() == 'objects'"
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("update-deny-existing-db").await;

        // First, create a new object (should succeed - !data.exists() && layer='objects')
        client
            .set(
                "/paths/page-123/path-456",
                json!({
                    "layer": "objects",
                    "x": 100
                }),
            )
            .await
            .expect("Initial set should succeed - new object with layer='objects'");

        // Now try to UPDATE the existing object
        // This should fail because data.exists() is now true
        let result = client
            .update("/paths/page-123/path-456", json!({ "x": 200 }))
            .await;

        assert!(
            result.is_err(),
            "UPDATE should fail - parent rule denies modification of existing objects"
        );
    });
}

/// Test that cascading .write rules use correct data/newData context at each level.
///
/// This tests a subtle bug: when checking write permission at a child path,
/// ancestor rules should be evaluated with data/newData at THEIR level,
/// not the target path's level.
///
/// Example: If we SET /a/b/c and there's a rule at /a/$x that checks
/// newData.child('foo'), it should look at newData at the /a/$x level,
/// not at /a/b/c.
#[test]
fn test_cascading_write_rules_use_correct_context() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules where the parent checks a sibling property
        server
            .set_rules(
                "cascade-context-db",
                json!({
                    "rules": {
                        ".read": true,
                        "items": {
                            "$itemId": {
                                // Parent rule: only allow writes if 'enabled' child is true
                                ".write": "newData.child('enabled').val() === true",
                                "name": {
                                    ".write": true
                                },
                                "enabled": {
                                    ".write": true
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("cascade-context-db").await;

        // SET the entire object with enabled=true
        // The $itemId rule should pass because newData.child('enabled').val() === true
        let result = client
            .set(
                "/items/item-1",
                json!({
                    "name": "Test Item",
                    "enabled": true
                }),
            )
            .await;

        assert!(
            result.is_ok(),
            "SET with enabled=true should succeed. Error: {:?}",
            result.err()
        );

        // Now UPDATE just the 'name' property
        // The $itemId rule should evaluate with newData being the MERGED result
        // (existing data + update), so newData.child('enabled') should still be true
        let result = client
            .update("/items/item-1", json!({ "name": "Updated Name" }))
            .await;

        assert!(
            result.is_ok(),
            "UPDATE name should succeed - merged newData still has enabled=true. Error: {:?}",
            result.err()
        );

        // Verify the update worked
        let value = client
            .once("/items/item-1")
            .await
            .expect("Should read item");
        assert_eq!(value["name"], json!("Updated Name"));
        assert_eq!(value["enabled"], json!(true));
    });
}

/// Test that SET to a child path evaluates parent rules with correct context.
///
/// When doing SET /parent/child/prop, if there's a rule at /parent/child
/// that checks newData.child('sibling'), it needs the merged view of newData
/// at the parent level, not just the value being written to 'prop'.
#[test]
fn test_set_to_child_evaluates_parent_rule_with_parent_context() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "set-child-context-db",
                json!({
                    "rules": {
                        ".read": true,
                        "users": {
                            "$uid": {
                                // Only allow writes if the 'active' field will be true after write
                                ".write": "newData.child('active').val() === true"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("set-child-context-db").await;

        // First create a user with active=true
        client
            .set(
                "/users/user-1",
                json!({
                    "name": "Alice",
                    "active": true
                }),
            )
            .await
            .expect("Creating user with active=true should succeed");

        // Now SET just the name property
        // This is a SET to /users/user-1/name with value "Bob"
        // The $uid rule checks newData.child('active').val() === true
        // For this to work, newData at the $uid level needs to be the merged result
        let result = client.set("/users/user-1/name", json!("Bob")).await;

        assert!(
            result.is_ok(),
            "SET to child path should succeed - parent rule should see merged newData with active=true. Error: {:?}",
            result.err()
        );

        // Verify the update
        let value = client
            .once("/users/user-1")
            .await
            .expect("Should read user");
        assert_eq!(value["name"], json!("Bob"));
        assert_eq!(value["active"], json!(true));
    });
}

/// Test .validate rules at different levels during UPDATE.
///
/// .validate rules should be checked at each level with the appropriate
/// data/newData context for that level.
#[test]
fn test_validate_rules_at_different_levels() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "validate-levels-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "posts": {
                            "$postId": {
                                // Validate that the post always has a title
                                ".validate": "newData.hasChild('title')",
                                "title": {
                                    // Validate title is a string
                                    ".validate": "newData.isString()"
                                },
                                "views": {
                                    // Validate views is a number
                                    ".validate": "newData.isNumber()"
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("validate-levels-db").await;

        // Create a valid post
        client
            .set(
                "/posts/post-1",
                json!({
                    "title": "Hello World",
                    "views": 0
                }),
            )
            .await
            .expect("Creating valid post should succeed");

        // UPDATE just the views - this should succeed because:
        // - The merged newData at $postId level still has 'title'
        // - The newData at 'views' level is a number
        let result = client.update("/posts/post-1", json!({ "views": 42 })).await;

        assert!(
            result.is_ok(),
            "UPDATE views should succeed - merged newData still has title. Error: {:?}",
            result.err()
        );

        // Verify
        let value = client
            .once("/posts/post-1")
            .await
            .expect("Should read post");
        assert_eq!(value["views"], json!(42));
        assert_eq!(value["title"], json!("Hello World"));
    });
}

// =============================================================================
// Comprehensive Rules Behavior Tests
// Based on Firebase documentation
// =============================================================================

// -----------------------------------------------------------------------------
// VALIDATE RULES - Don't Cascade
// -----------------------------------------------------------------------------

/// Test that .validate rules do NOT cascade - all levels must pass.
/// Unlike .write rules where a parent grant covers children,
/// .validate rules at each level must independently pass.
#[test]
fn test_validate_rules_do_not_cascade() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "validate-no-cascade-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "widget": {
                            // Parent validate: must have color and size
                            ".validate": "newData.hasChildren(['color', 'size'])",
                            "size": {
                                // Child validate: size must be a number 0-99
                                ".validate": "newData.isNumber() && newData.val() >= 0 && newData.val() <= 99"
                            },
                            "color": {
                                // Child validate: color must be a string
                                ".validate": "newData.isString()"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("validate-no-cascade-db").await;

        // SHOULD FAIL: missing color and size children
        let result = client.set("/widget", json!("foo")).await;
        assert!(
            result.is_err(),
            "Should fail - 'foo' doesn't have children color and size"
        );

        // SHOULD FAIL: has both children but size is invalid (not a number)
        let result = client
            .set("/widget", json!({"size": "foo", "color": "red"}))
            .await;
        assert!(result.is_err(), "Should fail - size is not a number");

        // SHOULD FAIL: has both children but size is out of range
        let result = client
            .set("/widget", json!({"size": 100, "color": "red"}))
            .await;
        assert!(result.is_err(), "Should fail - size is > 99");

        // SHOULD SUCCEED: valid widget
        let result = client
            .set("/widget", json!({"size": 50, "color": "blue"}))
            .await;
        assert!(
            result.is_ok(),
            "Should succeed - valid widget. Error: {:?}",
            result.err()
        );
    });
}

/// Test that .validate rules are skipped for deletes (null values).
/// "The .validate rules are only evaluated for non-null values"
#[test]
fn test_validate_rules_skipped_for_delete() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "validate-delete-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "widget": {
                            // This validate rule would fail for a delete (null has no children)
                            // But deletes should bypass .validate
                            ".validate": "newData.hasChildren(['color', 'size'])"
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("validate-delete-db").await;

        // First create a valid widget
        client
            .set("/widget", json!({"size": 50, "color": "blue"}))
            .await
            .expect("Creating widget should succeed");

        // Delete should succeed even though null doesn't have children
        // because .validate is not applied to deletes
        let result = client.remove("/widget").await;
        assert!(
            result.is_ok(),
            "Delete should succeed - .validate skipped for null. Error: {:?}",
            result.err()
        );

        // Verify deletion
        let value = client.once("/widget").await.expect("Should read");
        assert_eq!(value, json!(null));
    });
}

/// Test that all .validate rules in the hierarchy must pass.
/// Writing to a child must satisfy parent's .validate as well.
#[test]
fn test_validate_rules_all_levels_must_pass() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "validate-all-levels-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "widget": {
                            ".validate": "newData.hasChildren(['color', 'size'])",
                            "size": {
                                ".validate": "newData.isNumber() && newData.val() >= 0 && newData.val() <= 99"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("validate-all-levels-db").await;

        // If widget already exists with color, updating just size should work
        // because merged newData still has color
        client
            .set("/widget", json!({"size": 50, "color": "red"}))
            .await
            .expect("Initial set should succeed");

        // UPDATE just size - should succeed if parent validate sees merged data
        let result = client.update("/widget", json!({"size": 75})).await;
        assert!(
            result.is_ok(),
            "UPDATE size should succeed - merged newData has color. Error: {:?}",
            result.err()
        );

        // SET directly to /other-widget/size on a non-existent widget - this path
        // doesn't have any validate rules (no wildcard), so it should succeed
        // Note: This is testing that validate only applies to paths with rules
        let result = client.set("/other-widget/size", json!(50)).await;
        assert!(
            result.is_ok(),
            "SET to /other-widget/size should succeed - no validate rules at this path. Error: {:?}",
            result.err()
        );

        // The Firebase widget example test covers the case where writing to
        // /widget/size fails when widget doesn't exist (parent validate fails)
    });
}

// -----------------------------------------------------------------------------
// WRITE RULES - Cascade (Parent grants cover children)
// -----------------------------------------------------------------------------

/// Test that .write rules cascade - parent grant covers all children.
#[test]
fn test_write_rules_cascade_parent_grants() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "write-cascade-db",
                json!({
                    "rules": {
                        ".read": true,
                        "foo": {
                            // Parent grants write if baz child is true
                            ".write": "data.child('baz').val() === true",
                            "baz": {
                                // Allow setting up baz initially
                                ".write": true
                            },
                            "bar": {
                                // This would deny, but parent already granted
                                ".write": false
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("write-cascade-db").await;

        // First set up /foo/baz=true using the baz-specific rule
        client
            .set("/foo/baz", json!(true))
            .await
            .expect("Setting baz should succeed via baz's own rule");

        // Now the parent rule "data.child('baz').val() === true" should grant access
        // Writing to /foo/bar should succeed even though bar has ".write": false
        // because the parent already granted access
        let result = client.set("/foo/bar", json!("test")).await;
        assert!(
            result.is_ok(),
            "Write to /foo/bar should succeed - parent rule grants, child deny is ignored. Error: {:?}",
            result.err()
        );

        // Verify
        let value = client.once("/foo/bar").await.expect("Should read");
        assert_eq!(value, json!("test"));
    });
}

/// Test that child .write: false cannot revoke parent's grant.
#[test]
fn test_write_rules_child_cannot_revoke_parent() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "write-no-revoke-db",
                json!({
                    "rules": {
                        ".read": true,
                        "data": {
                            // Parent always grants
                            ".write": true,
                            "protected": {
                                // Child tries to deny - should be ignored
                                ".write": false
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("write-no-revoke-db").await;

        // Write to /data/protected should succeed because parent grants
        let result = client.set("/data/protected", json!("secret")).await;
        assert!(
            result.is_ok(),
            "Write should succeed - parent .write:true overrides child .write:false. Error: {:?}",
            result.err()
        );

        // Verify
        let value = client.once("/data/protected").await.expect("Should read");
        assert_eq!(value, json!("secret"));
    });
}

// -----------------------------------------------------------------------------
// OVERLAPPING RULES - Multiple rules on same node, ALL must pass
// -----------------------------------------------------------------------------

/// Test overlapping rules - both wildcard and specific must pass.
#[test]
fn test_overlapping_rules_all_must_pass() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "overlap-rules-db",
                json!({
                    "rules": {
                        ".read": true,
                        "messages": {
                            // Wildcard rule - always true
                            "$message": {
                                ".read": true,
                                ".write": true
                            },
                            // Specific rule for message1 - always false
                            "message1": {
                                ".read": false,
                                ".write": false
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("overlap-rules-db").await;

        // Write to message2 should succeed (only wildcard applies, which is true)
        let result = client.set("/messages/message2", json!("hello")).await;
        assert!(
            result.is_ok(),
            "Write to message2 should succeed. Error: {:?}",
            result.err()
        );

        // Write to message1 should FAIL because the specific rule is false
        // even though the wildcard rule is true
        let result = client.set("/messages/message1", json!("hello")).await;
        assert!(
            result.is_err(),
            "Write to message1 should fail - specific rule is false"
        );
    });
}

// -----------------------------------------------------------------------------
// newData IS MERGED RESULT
// -----------------------------------------------------------------------------

/// Test that newData in UPDATE is the merged result.
/// This is the core issue test - UPDATE with no parent write rule passing.
#[test]
fn test_update_newdata_is_merged_result() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "newdata-merged-db",
                json!({
                    "rules": {
                        ".read": true,
                        "items": {
                            "$itemId": {
                                // Only allow if merged result has both 'name' AND 'enabled'
                                ".write": "newData.hasChildren(['name', 'enabled'])"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("newdata-merged-db").await;

        // Create item with both fields
        let result = client
            .set("/items/item1", json!({"name": "Test", "enabled": true}))
            .await;
        assert!(
            result.is_ok(),
            "Initial set should succeed. Error: {:?}",
            result.err()
        );

        // UPDATE just the name - should succeed because MERGED newData
        // still has 'enabled' from existing data
        let result = client
            .update("/items/item1", json!({"name": "Updated"}))
            .await;
        assert!(
            result.is_ok(),
            "UPDATE should succeed - merged newData has both name and enabled. Error: {:?}",
            result.err()
        );

        // Verify
        let value = client.once("/items/item1").await.expect("Should read");
        assert_eq!(value["name"], json!("Updated"));
        assert_eq!(value["enabled"], json!(true));
    });
}

/// Test that newData.child() accesses the merged result in parent rules.
/// When updating a child property, parent rules should see merged newData.
#[test]
fn test_update_parent_rule_sees_merged_newdata_child() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "merged-child-db",
                json!({
                    "rules": {
                        ".read": true,
                        "users": {
                            "$uid": {
                                // Parent rule checks a specific child value
                                ".write": "newData.child('status').val() === 'active'"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("merged-child-db").await;

        // Create user with status=active
        let result = client
            .set("/users/u1", json!({"name": "Alice", "status": "active"}))
            .await;
        assert!(
            result.is_ok(),
            "Initial set should succeed. Error: {:?}",
            result.err()
        );

        // UPDATE just the name - should succeed because merged newData
        // still has status='active'
        let result = client.update("/users/u1", json!({"name": "Bob"})).await;
        assert!(
            result.is_ok(),
            "UPDATE name should succeed - merged newData.child('status') is still 'active'. Error: {:?}",
            result.err()
        );

        // UPDATE to change status to 'inactive' should fail
        let result = client
            .update("/users/u1", json!({"status": "inactive"}))
            .await;
        assert!(
            result.is_err(),
            "UPDATE status to 'inactive' should fail - rule requires status='active'"
        );
    });
}

// -----------------------------------------------------------------------------
// SET to child path - Parent rules need correct context
// This tests the core cascading context issue
// -----------------------------------------------------------------------------

/// Test SET to deep child path where ancestor rule checks newData.
/// The ancestor rule should see the merged state at its level.
#[test]
fn test_set_child_path_ancestor_rule_context() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "ancestor-context-db",
                json!({
                    "rules": {
                        ".read": true,
                        "profiles": {
                            "$uid": {
                                // Ancestor rule: profile must have 'verified' = true after write
                                ".write": "newData.child('verified').val() === true",
                                "settings": {
                                    "$setting": {
                                        ".write": true
                                    }
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("ancestor-context-db").await;

        // Create profile with verified=true
        client
            .set(
                "/profiles/p1",
                json!({"verified": true, "settings": {"theme": "dark"}}),
            )
            .await
            .expect("Initial set should succeed");

        // SET to /profiles/p1/settings/lang should succeed
        // because ancestor rule at $uid level should see merged newData
        // where verified is still true
        let result = client.set("/profiles/p1/settings/lang", json!("en")).await;
        assert!(
            result.is_ok(),
            "SET to settings/lang should succeed - ancestor sees merged newData with verified=true. Error: {:?}",
            result.err()
        );
    });
}

// -----------------------------------------------------------------------------
// VALIDATE vs WRITE behavior differences
// -----------------------------------------------------------------------------

/// Test that .write at child level grants access even if parent doesn't.
/// Unlike .validate, child .write rules can independently grant access.
#[test]
fn test_write_child_can_grant_independent_access() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "write-child-grant-db",
                json!({
                    "rules": {
                        ".read": true,
                        "data": {
                            // No .write at this level
                            "public": {
                                // Child grants access
                                ".write": true
                            },
                            "private": {
                                ".write": false
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("write-child-grant-db").await;

        // Write to /data/public should succeed (child grants)
        let result = client.set("/data/public", json!("hello")).await;
        assert!(
            result.is_ok(),
            "Write to /data/public should succeed. Error: {:?}",
            result.err()
        );

        // Write to /data/private should fail
        let result = client.set("/data/private", json!("secret")).await;
        assert!(result.is_err(), "Write to /data/private should fail");

        // Write to /data (parent) should fail - no rule grants access
        let result = client.set("/data", json!({"foo": "bar"})).await;
        assert!(
            result.is_err(),
            "Write to /data should fail - no rule at this level"
        );
    });
}

/// Test the Firebase widget example: .validate checks all levels.
#[test]
fn test_firebase_widget_validate_example() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "widget-validate-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "widget": {
                            ".validate": "newData.hasChildren(['color', 'size'])",
                            "size": {
                                ".validate": "newData.isNumber() && newData.val() >= 0 && newData.val() <= 99"
                            },
                            "color": {
                                ".validate": "newData.isString()"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("widget-validate-db").await;

        // These should all fail

        // FAIL: no children
        let result = client.set("/widget", json!("foo")).await;
        assert!(result.is_err(), "Should fail - no children color and size");

        // FAIL: missing color
        let result = client.set("/widget/size", json!(22)).await;
        assert!(
            result.is_err(),
            "Should fail - missing color (parent validate fails)"
        );

        // FAIL: size is not a number
        let result = client
            .set("/widget", json!({"size": "foo", "color": "red"}))
            .await;
        assert!(result.is_err(), "Should fail - size is not a number");

        // SUCCESS: valid widget
        let result = client
            .set("/widget", json!({"size": 21, "color": "blue"}))
            .await;
        assert!(
            result.is_ok(),
            "Should succeed - valid widget. Error: {:?}",
            result.err()
        );

        // If record exists with color, updating just size should work
        let result = client.set("/widget/size", json!(99)).await;
        assert!(
            result.is_ok(),
            "Should succeed - existing record has color, just updating size. Error: {:?}",
            result.err()
        );
    });
}

/// Test the Firebase widget example with .write (different behavior from .validate).
///
/// Key differences from .validate:
/// - .write rules cascade (if parent grants, children are allowed)
/// - Child .write rules can grant independent access even if parent rule would fail
#[test]
fn test_firebase_widget_write_example() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "widget-write-db",
                json!({
                    "rules": {
                        ".read": true,
                        // Note: NO root .write rule - each path must grant its own access
                        "widget": {
                            // a widget must have 'color' and 'size' in order to be written to this path
                            ".write": "newData.hasChildren(['color', 'size'])",
                            "size": {
                                // the value of "size" must be a number between 0 and 99,
                                // ONLY IF WE WRITE DIRECTLY TO SIZE
                                ".write": "newData.isNumber() && newData.val() >= 0 && newData.val() <= 99"
                            },
                            "color": {
                                // simplified example
                                ".write": "newData.isString()"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("widget-write-db").await;

        // TEST 1: Child rule grants independent access even when widget doesn't exist
        let result = client.set("/widget/size", json!(50)).await;
        assert!(
            result.is_ok(),
            "Child .write rule should grant access even when widget doesn't exist. Error: {:?}",
            result.err()
        );

        // Verify we created an "invalid" widget with just size (no color)
        let widget = client.once("/widget").await.expect("Should read widget");
        assert_eq!(widget, json!({"size": 50}), "Widget should only have size");

        // TEST 2: Parent rule cascade allows invalid child values
        let result = client
            .set("/widget", json!({"size": 99999, "color": "red"}))
            .await;
        assert!(
            result.is_ok(),
            "Parent .write should grant access even with invalid size (99999 > 99). Error: {:?}",
            result.err()
        );

        // Verify the invalid data was written
        let widget = client.once("/widget").await.expect("Should read widget");
        assert_eq!(widget, json!({"size": 99999, "color": "red"}));
    });
}

// -----------------------------------------------------------------------------
// READ RULES - Also cascade
// -----------------------------------------------------------------------------

/// Test that read rules cascade - parent grant covers children.
#[test]
fn test_read_rules_cascade() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "read-cascade-db",
                json!({
                    "rules": {
                        ".write": true,
                        "data": {
                            // Parent grants read
                            ".read": true,
                            "secret": {
                                // Child tries to deny - should be ignored
                                ".read": false
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("read-cascade-db").await;

        // Set up data
        client
            .set("/data/secret", json!("hidden"))
            .await
            .expect("Write should succeed");

        // Read /data/secret should succeed because parent grants
        let result = client.once("/data/secret").await;
        assert!(
            result.is_ok(),
            "Read should succeed - parent .read:true overrides child .read:false. Error: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), json!("hidden"));
    });
}

/// Test that read rules are atomic - can't read parent if any child denies.
#[test]
fn test_read_rules_atomic_no_filtering() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "read-atomic-db",
                json!({
                    "rules": {
                        ".write": true,
                        "records": {
                            "rec1": {
                                ".read": true
                            },
                            "rec2": {
                                ".read": false
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("read-atomic-db").await;

        // Set up data
        client
            .set("/records/rec1", json!("data1"))
            .await
            .expect("Write should succeed");
        client
            .set("/records/rec2", json!("data2"))
            .await
            .expect("Write should succeed");

        // Read /records should FAIL because rec2 denies
        // Rules don't filter - they fail atomically
        let result = client.once("/records").await;
        assert!(
            result.is_err(),
            "Read /records should fail - rec2 denies, rules don't filter"
        );

        // Read /records/rec1 directly should succeed
        let result = client.once("/records/rec1").await;
        assert!(
            result.is_ok(),
            "Read /records/rec1 directly should succeed. Error: {:?}",
            result.err()
        );
    });
}

// -----------------------------------------------------------------------------
// REFERENCING DATA IN OTHER PATHS
// -----------------------------------------------------------------------------

/// Test using root to check data elsewhere in the database.
#[test]
fn test_root_reference_in_rules() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "root-ref-db",
                json!({
                    "rules": {
                        ".read": true,
                        "allow_writes": {
                            ".write": true
                        },
                        "data": {
                            // Only allow writes if /allow_writes is true
                            ".write": "root.child('allow_writes').val() === true"
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("root-ref-db").await;

        // Initially allow_writes doesn't exist, so write to data should fail
        let result = client.set("/data/test", json!("value")).await;
        assert!(
            result.is_err(),
            "Write should fail - allow_writes is not true"
        );

        // Set allow_writes to true
        client
            .set("/allow_writes", json!(true))
            .await
            .expect("Setting allow_writes should succeed");

        // Now write to data should succeed
        let result = client.set("/data/test", json!("value")).await;
        assert!(
            result.is_ok(),
            "Write should succeed - allow_writes is true. Error: {:?}",
            result.err()
        );
    });
}

/// Test using data.parent() to check parent node.
#[test]
fn test_data_parent_in_rules() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "parent-ref-db",
                json!({
                    "rules": {
                        ".read": true,
                        // No root .write - each path must grant its own access
                        "items": {
                            "$itemId": {
                                "content": {
                                    // Only allow write if parent doesn't have readOnly flag
                                    ".write": "!data.parent().child('readOnly').exists()"
                                },
                                "readOnly": {
                                    ".write": true
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("parent-ref-db").await;

        // Create an item without readOnly
        client
            .set("/items/item1/content", json!("initial"))
            .await
            .expect("Initial write should succeed");

        // Add readOnly flag
        client
            .set("/items/item1/readOnly", json!(true))
            .await
            .expect("Setting readOnly should succeed");

        // Now writing to content should fail
        let result = client.set("/items/item1/content", json!("updated")).await;
        assert!(
            result.is_err(),
            "Write should fail - parent has readOnly flag"
        );
    });
}

// -----------------------------------------------------------------------------
// CREATE vs UPDATE vs DELETE semantics
// -----------------------------------------------------------------------------

/// Test rule that allows create and delete but not update.
#[test]
fn test_create_or_delete_only() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "create-delete-db",
                json!({
                    "rules": {
                        ".read": true,
                        "records": {
                            "$recordId": {
                                // Allow create (data doesn't exist) or delete (newData is null)
                                // but not update (both exist)
                                ".write": "!data.exists() || !newData.exists()"
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("create-delete-db").await;

        // CREATE should succeed
        let result = client.set("/records/r1", json!({"name": "test"})).await;
        assert!(
            result.is_ok(),
            "Create should succeed. Error: {:?}",
            result.err()
        );

        // UPDATE should fail (both data and newData exist)
        let result = client.set("/records/r1", json!({"name": "updated"})).await;
        assert!(
            result.is_err(),
            "Update should fail - both data and newData exist"
        );

        // DELETE should succeed
        let result = client.remove("/records/r1").await;
        assert!(
            result.is_ok(),
            "Delete should succeed. Error: {:?}",
            result.err()
        );
    });
}

// -----------------------------------------------------------------------------
// Multi-level validate with different contexts
// -----------------------------------------------------------------------------

/// Test multi-level validate where each level needs correct newData context.
#[test]
fn test_multilevel_validate_contexts() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "multilevel-validate-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "orders": {
                            "$orderId": {
                                // Order must have 'items' and 'total'
                                ".validate": "newData.hasChildren(['items', 'total'])",
                                "items": {
                                    // Items must be an object (has children)
                                    ".validate": "newData.hasChildren()"
                                },
                                "total": {
                                    // Total must be a number
                                    ".validate": "newData.isNumber()"
                                },
                                "status": {
                                    // Status must be a string
                                    ".validate": "newData.isString()"
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("multilevel-validate-db").await;

        // Create valid order
        let result = client
            .set(
                "/orders/o1",
                json!({
                    "items": {"item1": 2, "item2": 1},
                    "total": 99.99,
                    "status": "pending"
                }),
            )
            .await;
        assert!(
            result.is_ok(),
            "Creating valid order should succeed. Error: {:?}",
            result.err()
        );

        // UPDATE just status - should succeed because merged newData
        // still has items and total
        let result = client
            .update("/orders/o1", json!({"status": "shipped"}))
            .await;
        assert!(
            result.is_ok(),
            "UPDATE status should succeed. Error: {:?}",
            result.err()
        );

        // UPDATE with invalid total (string instead of number)
        let result = client
            .update("/orders/o1", json!({"total": "invalid"}))
            .await;
        assert!(
            result.is_err(),
            "UPDATE with invalid total should fail - total.validate requires number"
        );
    });
}

// -----------------------------------------------------------------------------
// Validate on child only (parent has no .validate)
// -----------------------------------------------------------------------------

/// Test that child .validate rules fire when SET is done at a parent path,
/// even when the parent node itself has no .validate rule.
/// This exercises the validate_children recursive walk — the optimization
/// (has_validate_below) must still allow the child rule to fire.
#[test]
fn test_validate_child_only_fires_on_parent_set() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "validate-child-only-db",
                json!({
                    "rules": {
                        ".read": true,
                        ".write": true,
                        "profiles": {
                            "$uid": {
                                // NO .validate on the $uid node itself
                                "age": {
                                    ".validate": "newData.isNumber() && newData.val() >= 0 && newData.val() <= 150"
                                },
                                "name": {
                                    ".validate": "newData.isString()"
                                }
                            }
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("validate-child-only-db").await;

        // SET at parent with valid children — should succeed
        let result = client
            .set("/profiles/alice", json!({"name": "Alice", "age": 30}))
            .await;
        assert!(
            result.is_ok(),
            "Valid profile should succeed. Error: {:?}",
            result.err()
        );

        // SET at parent with invalid age (string instead of number) — child validate should reject
        let result = client
            .set("/profiles/bob", json!({"name": "Bob", "age": "old"}))
            .await;
        assert!(
            result.is_err(),
            "Should fail - age validate requires a number"
        );

        // SET at parent with invalid name (number instead of string) — child validate should reject
        let result = client
            .set("/profiles/charlie", json!({"name": 42, "age": 25}))
            .await;
        assert!(
            result.is_err(),
            "Should fail - name validate requires a string"
        );

        // SET at parent with age out of range — child validate should reject
        let result = client
            .set("/profiles/dave", json!({"name": "Dave", "age": 200}))
            .await;
        assert!(result.is_err(), "Should fail - age validate requires 0-150");

        // SET at parent with extra field (no validate rule) — should succeed
        let result = client
            .set(
                "/profiles/eve",
                json!({"name": "Eve", "age": 25, "bio": "hello"}),
            )
            .await;
        assert!(
            result.is_ok(),
            "Extra fields without validate rules should be allowed. Error: {:?}",
            result.err()
        );
    });
}

// =============================================================================
// Hot-reload (CONFIG_PUSH) Tests
// =============================================================================

/// A CONFIG_PUSH arriving after a database is already loaded should change
/// the rules in effect for subsequent requests, without tearing the DB down.
#[test]
fn test_rules_hot_reload_on_config_push() {
    run_test(|| async {
        let server = TestServer::new();

        // Start permissive — anonymous writes allowed.
        server
            .set_rules(
                "hotreload-proj",
                json!({"rules": {".read": true, ".write": true}}),
            )
            .expect("initial rules");

        let mut client = server.client();
        client.connect("hotreload-proj/room-a").await;

        client
            .set("/msg", "before")
            .await
            .expect("write allowed under permissive rules");

        // Hot-push strict rules. The database is already running.
        server
            .push_rules(
                "hotreload-proj",
                json!({"rules": {".read": true, ".write": false}}),
            )
            .expect("push strict rules");

        // Next write should be rejected — the running DB must have adopted
        // the new evaluator.
        let err = client
            .set("/msg", "after")
            .await
            .expect_err("write should be denied after hot-reloading deny-writes rules");
        assert!(
            err.to_lowercase().contains("permission") || err.to_lowercase().contains("denied"),
            "expected permission denied, got: {err}"
        );

        // The old value from before the hot-reload must still be in place.
        let current = client.once("/msg").await.expect("read");
        assert_eq!(current, json!("before"));

        // Loosen again — next write should succeed.
        server
            .push_rules(
                "hotreload-proj",
                json!({"rules": {".read": true, ".write": true}}),
            )
            .expect("push permissive rules");

        client
            .set("/msg", "after2")
            .await
            .expect("write allowed after loosening rules");
        let current = client.once("/msg").await.expect("read");
        assert_eq!(current, json!("after2"));
    });
}

/// Pushing `Value::Null` via `push_rules` clears rules entirely (fully open),
/// matching the behaviour of a CONFIG_PUSH that carries no `rules` field.
#[test]
fn test_rules_hot_reload_clear() {
    run_test(|| async {
        let server = TestServer::new();

        server
            .set_rules(
                "hotreload-clear",
                json!({"rules": {".read": true, ".write": false}}),
            )
            .expect("initial strict rules");

        let mut client = server.client();
        client.connect("hotreload-clear/room").await;

        // Strict — write denied.
        let err = client.set("/x", 1).await.expect_err("denied under strict");
        assert!(err.to_lowercase().contains("permission") || err.to_lowercase().contains("denied"));

        // Clear rules (push Null) — should now be fully open.
        server
            .push_rules("hotreload-clear", serde_json::Value::Null)
            .expect("clear rules");

        client
            .set("/x", 1)
            .await
            .expect("write allowed after clearing rules");
    });
}
