// Package testutil provides test utilities and fixtures for the Lark proxy.
//
// # Overview
//
// This package contains:
//   - Test constants (IDs, secrets, etc.)
//   - Factory functions for creating test objects
//   - Database setup helpers for integration tests
//
// # Usage
//
// Import this package in your test files:
//
//	import "github.com/lark-sh/lark/edge/testutil"
//
//	func TestSomething(t *testing.T) {
//	    project := testutil.CreateTestProject()
//	    server := testutil.CreateTestServer()
//	    // ...
//	}
//
// # Integration Tests
//
// For tests that need a real database, use SetupTestDB:
//
//	func TestWithDatabase(t *testing.T) {
//	    db := testutil.SetupTestDB(t)  // Uses TEST_DATABASE_URL env var
//	    defer db.Close()
//	    // ...
//	}
//
// If TEST_DATABASE_URL is not set, tests that require a database are skipped.
//
// # Test Constants
//
// Standard test values are provided for consistency:
//   - TestProjectID, TestDatabaseID, TestServerID
//   - TestSecretKey, TestAdminSecretKey, TestServerSecret
//   - TestAccountID, TestSessionID
package testutil

import (
	"encoding/json"
	"time"

	"github.com/lark-sh/lark/edge/db"
)

// Test constants
const (
	TestProjectID      = "test-project"
	TestDatabaseID     = "test-database"
	TestServerID       = "test-server"
	TestAccountID      = "test-account"
	TestSessionID      = "test-session"
	TestSecretKey      = "test-secret-key-12345"
	TestAdminSecretKey = "test-admin-secret-key-12345"
	TestServerSecret   = "server-secret-12345"
)

// CreateTestProject creates a test project with default values
func CreateTestProject() *db.Project {
	return &db.Project{
		ID:                            TestProjectID,
		Name:                          "Test Project",
		SecretKey:                     TestSecretKey,
		AdminSecretKey:                TestAdminSecretKey,
		RulesJSON:                     `{".read": true, ".write": true}`,
		Ephemeral:                     false,
		AutoCreate:                    true,
		FirebaseCompatEnabled:         true,
		UseFirstPathSegmentAsDatabase: true,
		CreatedAt:                     NowMS(),
		UpdatedAt:                     NowMS(),
	}
}

// CreateTestServer creates a test server with default values
func CreateTestServer() *db.Server {
	return &db.Server{
		ID:              TestServerID,
		Hostname:        "test-server.example.com",
		IPAddress:       "192.168.1.100",
		UDPPort:         7777,
		LastHeartbeat:   NowMS(),
		DatabaseCount:   0,
		ConnectionCount: 0,
		Capacity:        10000,
		Status:          "active",
	}
}

// CreateTestDatabase creates a test database with default values
func CreateTestDatabase() *db.Database {
	return &db.Database{
		ProjectID:    TestProjectID,
		ID:           TestDatabaseID,
		ServerID:     TestServerID,
		Ephemeral:    false,
		Status:       "active",
		LastActivity: NowMS(),
		CreatedAt:    NowMS(),
	}
}

// NowMS returns the current time in milliseconds
func NowMS() int64 {
	return time.Now().UnixMilli()
}

// MustMarshalJSON marshals v to JSON, panicking on error
func MustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// LarkJoinMessage creates a Lark protocol join message
// Format: {"o": "j", "d": "project/database", "r": "r1"}
func LarkJoinMessage(project, database string) []byte {
	return MustMarshalJSON(map[string]interface{}{
		"o": "j",
		"d": project + "/" + database,
		"r": "r1",
	})
}

// FirebaseAuthMessage creates a Firebase protocol auth message
func FirebaseAuthMessage(requestID int, token string) []byte {
	return MustMarshalJSON(map[string]interface{}{
		"t": "d",
		"d": map[string]interface{}{
			"r": requestID,
			"a": "auth",
			"b": map[string]interface{}{
				"cred": token,
			},
		},
	})
}

// FirebaseQueryMessage creates a Firebase protocol query message
func FirebaseQueryMessage(requestID int, path string) []byte {
	return MustMarshalJSON(map[string]interface{}{
		"t": "d",
		"d": map[string]interface{}{
			"r": requestID,
			"a": "q",
			"b": map[string]interface{}{
				"p": path,
				"h": "",
			},
		},
	})
}

// FirebaseStatsMessage creates a Firebase protocol stats message
func FirebaseStatsMessage(requestID int) []byte {
	return MustMarshalJSON(map[string]interface{}{
		"t": "d",
		"d": map[string]interface{}{
			"r": requestID,
			"a": "s",
			"b": map[string]interface{}{
				"c": map[string]interface{}{},
			},
		},
	})
}

// FirebaseSwitchAckMessage creates a Firebase SWITCH_ACK control message
// Sent by client on WS after upgrading from LP
func FirebaseSwitchAckMessage() []byte {
	return MustMarshalJSON(map[string]interface{}{
		"t": "c",
		"d": map[string]interface{}{
			"t": "a",
			"d": map[string]interface{}{},
		},
	})
}

// FirebaseEndTransmissionMessage creates a Firebase END_TRANSMISSION control message
// Sent by client on LP after SWITCH_ACK to signal LP connection should close
func FirebaseEndTransmissionMessage() []byte {
	return MustMarshalJSON(map[string]interface{}{
		"t": "c",
		"d": map[string]interface{}{
			"t": "n",
			"d": map[string]interface{}{},
		},
	})
}
