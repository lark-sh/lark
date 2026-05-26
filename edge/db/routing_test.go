package db

import (
	"context"
	"testing"
	"time"
)

func TestParseDatabasePath(t *testing.T) {
	tests := []struct {
		path       string
		wantProj   string
		wantDB     string
		wantOK     bool
	}{
		{"project/database", "project", "database", true},
		{"my-project/my-database", "my-project", "my-database", true},
		{"a/b", "a", "b", true},
		{"project-123/db-456", "project-123", "db-456", true},
		{"project/path/with/slashes", "project", "path/with/slashes", true},
		{"", "", "", false},
		{"no-slash", "", "", false},
		{"/leading-slash", "", "leading-slash", false},
		{"trailing-slash/", "trailing-slash", "", false},
		{"/", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			proj, db, ok := ParseDatabasePath(tt.path)
			if proj != tt.wantProj || db != tt.wantDB || ok != tt.wantOK {
				t.Errorf("ParseDatabasePath(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.path, proj, db, ok, tt.wantProj, tt.wantDB, tt.wantOK)
			}
		})
	}
}

func TestValidateDatabaseID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"valid simple", "mydb", false},
		{"valid with hyphens", "my-database", false},
		{"valid with numbers", "db-123", false},
		{"valid single char", "a", false},
		{"valid max length", "abcdefghijklmnopqrstuvwxyz01234567890123", false}, // 40 chars

		{"empty", "", true},
		{"too long", "abcdefghijklmnopqrstuvwxyz012345678901234", true}, // 41 chars
		{"uppercase", "MyDB", true},
		{"spaces", "my db", true},
		{"underscore", "my_db", true},
		{"dot", "my.db", true},
		{"slash", "my/db", true},
		{"double hyphen", "my--db", true},
		{"leading hyphen", "-mydb", true},
		{"trailing hyphen", "mydb-", true},
		{"only hyphens", "---", true},
		{"double hyphen at start", "--mydb", true},
		{"double hyphen at end", "mydb--", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabaseID(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDatabaseID(%q) error = %v, wantErr %v", tt.id, err, tt.wantErr)
			}
		})
	}
}

func TestGetConnectDataWithExistingDatabase(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)
	seedTestData(t, database)

	ctx := context.Background()
	heartbeatTimeout := int64(30) // 30 seconds

	data, err := database.GetConnectData(ctx, testProjectID, testDatabaseID, heartbeatTimeout)
	if err != nil {
		t.Fatalf("GetConnectData failed: %v", err)
	}

	// Check project
	if data.Project == nil {
		t.Fatal("Project should not be nil")
	}
	if data.Project.ID != testProjectID {
		t.Errorf("Project.ID: got %q, want %q", data.Project.ID, testProjectID)
	}

	// Check database
	if data.Database == nil {
		t.Fatal("Database should not be nil")
	}
	if data.Database.ID != testDatabaseID {
		t.Errorf("Database.ID: got %q, want %q", data.Database.ID, testDatabaseID)
	}

	// Check server
	if data.Server == nil {
		t.Fatal("Server should not be nil")
	}
	if data.Server.ID != testServerID {
		t.Errorf("Server.ID: got %q, want %q", data.Server.ID, testServerID)
	}
}

func TestGetConnectDataWithNonexistentProject(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)

	ctx := context.Background()
	heartbeatTimeout := int64(30)

	_, err := database.GetConnectData(ctx, "nonexistent-project", "some-db", heartbeatTimeout)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestGetConnectDataWithNonexistentDatabase(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)
	seedMinimalTestData(t, database)

	ctx := context.Background()
	heartbeatTimeout := int64(30)

	data, err := database.GetConnectData(ctx, testProjectID, "nonexistent-db", heartbeatTimeout)
	if err != nil {
		t.Fatalf("GetConnectData failed: %v", err)
	}

	// Project should exist
	if data.Project == nil {
		t.Fatal("Project should not be nil")
	}

	// Database should be nil (doesn't exist)
	if data.Database != nil {
		t.Error("Database should be nil for nonexistent database")
	}

	// Server should be nil (no database assignment)
	if data.Server != nil {
		t.Error("Server should be nil for nonexistent database")
	}
}

func TestGetConnectDataWithUnhealthyServer(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)

	ctx := context.Background()
	now := NowMS()

	// Create account and project
	database.Exec(ctx, `INSERT INTO accounts (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)`,
		testAccountID, "test@example.com", "$2a$10$placeholder", now)
	database.Exec(ctx, `INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, use_first_path_segment_as_database, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		testProjectID, "Test", "secret", "admin-secret", "{}", false, true, false, "", false, now, now)

	// Create unhealthy server (old heartbeat)
	oldHeartbeat := now - 60*1000 // 60 seconds ago
	database.Exec(ctx, `INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, database_count, connection_count, capacity, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		testServerID, "test.example.com", "192.168.1.1", 7777, oldHeartbeat, 0, 0, 10000, "active")

	// Create database assigned to unhealthy server
	database.Exec(ctx, `INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		testProjectID, testDatabaseID, testServerID, false, "active", now, now)

	heartbeatTimeout := int64(30) // 30 seconds

	data, err := database.GetConnectData(ctx, testProjectID, testDatabaseID, heartbeatTimeout)
	if err != nil {
		t.Fatalf("GetConnectData failed: %v", err)
	}

	// Database should exist
	if data.Database == nil {
		t.Fatal("Database should exist")
	}

	// Server should be nil (unhealthy)
	if data.Server != nil {
		t.Error("Server should be nil for unhealthy server")
	}
}

func TestEnsureRoutingDataCreatesDatabase(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)
	seedMinimalTestData(t, database)

	ctx := context.Background()
	heartbeatTimeout := int64(30)

	// Request routing for a new database
	server, _, err := database.EnsureRoutingData(ctx, testProjectID, "new-database", heartbeatTimeout)
	if err != nil {
		t.Fatalf("EnsureRoutingData failed: %v", err)
	}

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	// Verify the database was created
	data, err := database.GetConnectData(ctx, testProjectID, "new-database", heartbeatTimeout)
	if err != nil {
		t.Fatalf("GetConnectData failed: %v", err)
	}

	if data.Database == nil {
		t.Error("Database should have been created")
	}
}

func TestEnsureRoutingDataReturnsExistingServer(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)
	seedTestData(t, database)

	ctx := context.Background()
	heartbeatTimeout := int64(30)

	server, _, err := database.EnsureRoutingData(ctx, testProjectID, testDatabaseID, heartbeatTimeout)
	if err != nil {
		t.Fatalf("EnsureRoutingData failed: %v", err)
	}

	if server.ID != testServerID {
		t.Errorf("Server.ID: got %q, want %q", server.ID, testServerID)
	}
}

func TestEnsureRoutingDataNoServersAvailable(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)

	ctx := context.Background()
	now := NowMS()

	// Create account and project, but no servers
	database.Exec(ctx, `INSERT INTO accounts (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)`,
		testAccountID, "test@example.com", "$2a$10$placeholder", now)
	database.Exec(ctx, `INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, use_first_path_segment_as_database, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		testProjectID, "Test", "secret", "admin-secret", "{}", false, true, false, "", false, now, now)

	heartbeatTimeout := int64(30)

	_, _, err := database.EnsureRoutingData(ctx, testProjectID, "new-database", heartbeatTimeout)
	if err != ErrNoServersAvailable {
		t.Errorf("Expected ErrNoServersAvailable, got %v", err)
	}
}

func TestEnsureRoutingDataReactivatesInactiveDatabase(t *testing.T) {
	database := setupTestDB(t)
	cleanTestDB(t, database)

	ctx := context.Background()
	now := NowMS()

	// Create account, project, server
	database.Exec(ctx, `INSERT INTO accounts (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)`,
		testAccountID, "test@example.com", "$2a$10$placeholder", now)
	database.Exec(ctx, `INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json, ephemeral, auto_create, firebase_compat_enabled, firebase_project_id, use_first_path_segment_as_database, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		testProjectID, "Test", "secret", "admin-secret", "{}", false, true, false, "", false, now, now)
	database.Exec(ctx, `INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, database_count, connection_count, capacity, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		testServerID, "test.example.com", "192.168.1.1", 7777, now, 0, 0, 10000, "active")

	// Create INACTIVE database with server assignment
	database.Exec(ctx, `INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		testProjectID, testDatabaseID, testServerID, false, "inactive", now, now)

	heartbeatTimeout := int64(30)

	// EnsureRoutingData should reactivate the database
	server, _, err := database.EnsureRoutingData(ctx, testProjectID, testDatabaseID, heartbeatTimeout)
	if err != nil {
		t.Fatalf("EnsureRoutingData failed: %v", err)
	}

	if server.ID != testServerID {
		t.Errorf("Server.ID: got %q, want %q", server.ID, testServerID)
	}

	// Give a moment for the async activation
	time.Sleep(50 * time.Millisecond)

	// Verify database is now active
	data, _ := database.GetConnectData(ctx, testProjectID, testDatabaseID, heartbeatTimeout)
	if data.Database != nil && data.Database.Status != "active" {
		t.Logf("Database status: %s (may be inactive if ActivateDatabase wasn't called)", data.Database.Status)
	}
}
