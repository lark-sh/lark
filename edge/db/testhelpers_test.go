package db

import (
	"context"
	"os"
	"testing"
	"time"
)

const (
	testProjectID      = "test-project"
	testDatabaseID     = "test-database"
	testServerID       = "test-server"
	testAccountID      = "test-account"
	testSecretKey      = "test-secret-key-12345"
	testAdminSecretKey = "test-admin-secret-key-12345"
)

// setupTestDB creates a test database connection.
// Skips the test if TEST_DATABASE_URL is not set.
func setupTestDB(t *testing.T) *PostgresDB {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	database, err := NewPostgres(ctx, url)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	t.Cleanup(func() {
		database.Close()
	})

	return database
}

// cleanTestDB truncates all tables for test isolation.
func cleanTestDB(t *testing.T, database *PostgresDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tables := []string{
		"databases",
		"sessions",
		"projects",
		"servers",
		"accounts",
	}

	for _, table := range tables {
		_, err := database.Exec(ctx, "TRUNCATE "+table+" CASCADE")
		if err != nil {
			t.Fatalf("Failed to truncate %s: %v", table, err)
		}
	}
}

// seedTestData inserts common test data into the database.
func seedTestData(t *testing.T, database *PostgresDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := NowMS()

	_, err := database.Exec(ctx, `
		INSERT INTO accounts (id, email, password_hash, created_at)
		VALUES ($1, $2, $3, $4)
	`, testAccountID, "test@example.com", "$2a$10$placeholder", now)
	if err != nil {
		t.Fatalf("Failed to create test account: %v", err)
	}

	_, err = database.Exec(ctx, `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, use_first_path_segment_as_database, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, testProjectID, "Test Project", testSecretKey, testAdminSecretKey, `{".read": true}`, false, true, true, "", true, now, now)
	if err != nil {
		t.Fatalf("Failed to create test project: %v", err)
	}

	_, err = database.Exec(ctx, `
		INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, database_count, connection_count, capacity, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, testServerID, "test-server", "127.0.0.1", 7777, now, 0, 0, 10000, "active")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}

	_, err = database.Exec(ctx, `
		INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, testProjectID, testDatabaseID, testServerID, false, "active", now, now)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
}

// seedMinimalTestData inserts only the minimum data needed for routing tests.
func seedMinimalTestData(t *testing.T, database *PostgresDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := NowMS()

	_, err := database.Exec(ctx, `
		INSERT INTO accounts (id, email, password_hash, created_at)
		VALUES ($1, $2, $3, $4)
	`, testAccountID, "test@example.com", "$2a$10$placeholder", now)
	if err != nil {
		t.Fatalf("Failed to create test account: %v", err)
	}

	_, err = database.Exec(ctx, `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, use_first_path_segment_as_database, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, testProjectID, "Test Project", testSecretKey, testAdminSecretKey, `{".read": true}`, false, true, true, "", true, now, now)
	if err != nil {
		t.Fatalf("Failed to create test project: %v", err)
	}

	_, err = database.Exec(ctx, `
		INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, database_count, connection_count, capacity, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, testServerID, "test-server", "127.0.0.1", 7777, now, 0, 0, 10000, "active")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
}
