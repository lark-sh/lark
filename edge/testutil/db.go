package testutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lark-sh/lark/edge/db"
)

// SetupTestDB creates a test database connection.
// Skips the test if TEST_DATABASE_URL is not set.
func SetupTestDB(t *testing.T) *db.PostgresDB {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	database, err := db.NewPostgres(ctx, url)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		database.Close()
	})

	return database
}

// CleanTestDB truncates all tables for test isolation.
// Should be called at the start of each test.
func CleanTestDB(t *testing.T, database *db.PostgresDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Truncate tables in order that respects foreign key constraints
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

// SeedTestData inserts common test data into the database.
func SeedTestData(t *testing.T, database *db.PostgresDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := NowMS()

	_, err := database.Exec(ctx, `
		INSERT INTO accounts (id, email, password_hash, created_at)
		VALUES ($1, $2, $3, $4)
	`, TestAccountID, "test@example.com", "$2a$10$placeholder", now)
	if err != nil {
		t.Fatalf("Failed to create test account: %v", err)
	}

	// Create test project
	_, err = database.Exec(ctx, `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, TestProjectID, "Test Project", TestSecretKey, TestAdminSecretKey, `{".read": true}`, false, true, true, "", now, now)
	if err != nil {
		t.Fatalf("Failed to create test project: %v", err)
	}

	// Create test server
	_, err = database.Exec(ctx, `
		INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, database_count, connection_count, capacity, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, TestServerID, "test-server.example.com", "192.168.1.100", 7777, now, 0, 0, 10000, "active")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}

	// Create test database
	_, err = database.Exec(ctx, `
		INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, TestProjectID, TestDatabaseID, TestServerID, false, "active", now, now)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	_, err = database.Exec(ctx, `
		INSERT INTO sessions (id, public_id, account_id, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`, TestSessionID, TestSessionID+"-pub", TestAccountID, now+24*60*60*1000, now)
	if err != nil {
		t.Fatalf("Failed to create test session: %v", err)
	}
}

// SeedMinimalTestData inserts only the minimum data needed for routing tests.
func SeedMinimalTestData(t *testing.T, database *db.PostgresDB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := NowMS()

	_, err := database.Exec(ctx, `
		INSERT INTO accounts (id, email, password_hash, created_at)
		VALUES ($1, $2, $3, $4)
	`, TestAccountID, "test@example.com", "$2a$10$placeholder", now)
	if err != nil {
		t.Fatalf("Failed to create test account: %v", err)
	}

	// Create test project with auto_create enabled
	_, err = database.Exec(ctx, `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, TestProjectID, "Test Project", TestSecretKey, TestAdminSecretKey, `{".read": true}`, false, true, true, "", now, now)
	if err != nil {
		t.Fatalf("Failed to create test project: %v", err)
	}

	// Create test server
	_, err = database.Exec(ctx, `
		INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, database_count, connection_count, capacity, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, TestServerID, "test-server.example.com", "192.168.1.100", 7777, now, 0, 0, 10000, "active")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
}
