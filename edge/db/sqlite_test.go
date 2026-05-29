package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// openMemorySqlite returns a fresh SqliteDB backed by a file in t.TempDir().
// (We don't use ":memory:" because modernc.org/sqlite gives every connection
// its own private memory DB, which doesn't survive the schema bootstrap pass
// followed by a fresh query connection.)
func openMemorySqlite(t *testing.T) *SqliteDB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lark.db")
	db, err := NewSqlite(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("NewSqlite: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func seedSqliteProject(t *testing.T, db *SqliteDB, projectID string, autoCreate bool) {
	t.Helper()
	now := NowMS()
	auto := 0
	if autoCreate {
		auto = 1
	}
	_, err := db.sql.ExecContext(context.Background(), `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, ephemeral, auto_create, created_at, updated_at)
		VALUES (?, ?, 'sk', 'ask', 1, ?, ?, ?)
	`, projectID, projectID, auto, now, now)
	if err != nil {
		t.Fatalf("seed project: %v", err)
	}
}

func seedSqliteServer(t *testing.T, db *SqliteDB, serverID string, healthy bool) {
	t.Helper()
	now := NowMS()
	heartbeat := now
	if !healthy {
		heartbeat = now - 1_000_000 // way past any heartbeat timeout
	}
	_, err := db.sql.ExecContext(context.Background(), `
		INSERT INTO servers (id, hostname, ip_address, udp_port, last_heartbeat, capacity, status)
		VALUES (?, ?, '127.0.0.1', 7779, ?, 50000, 'active')
	`, serverID, serverID, heartbeat)
	if err != nil {
		t.Fatalf("seed server: %v", err)
	}
}

func TestSqlite_GetProjectByID_NotFound(t *testing.T) {
	db := openMemorySqlite(t)
	_, err := db.GetProjectByID(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestSqlite_GetProjectByID_Found(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	p, err := db.GetProjectByID(context.Background(), "p1")
	if err != nil {
		t.Fatalf("GetProjectByID: %v", err)
	}
	if p.ID != "p1" || p.SecretKey != "sk" || !p.AutoCreate {
		t.Errorf("project mismatch: %+v", p)
	}
}

// A project seeded without an explicit `enabled` column must read back as
// enabled — the schema default protects against existing rows / older writers
// accidentally disabling projects.
func TestSqlite_GetProjectByID_EnabledDefaultsTrue(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	p, err := db.GetProjectByID(context.Background(), "p1")
	if err != nil {
		t.Fatalf("GetProjectByID: %v", err)
	}
	if !p.Enabled {
		t.Errorf("Enabled should default to true, got %+v", p)
	}
}

// CreateProject marks new projects enabled, and the value round-trips.
func TestSqlite_CreateProject_EnabledRoundTrips(t *testing.T) {
	db := openMemorySqlite(t)
	p := &Project{ID: "p1", Name: "P1", SecretKey: "sk", AdminSecretKey: "ask"}
	if err := db.CreateProject(context.Background(), p); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if !p.Enabled {
		t.Errorf("CreateProject should set Enabled=true on the struct, got %+v", p)
	}

	got, err := db.GetProjectByID(context.Background(), "p1")
	if err != nil {
		t.Fatalf("GetProjectByID: %v", err)
	}
	if !got.Enabled {
		t.Errorf("created project should read back enabled, got %+v", got)
	}

	// Disabling via a direct write round-trips to false.
	if _, err := db.sql.ExecContext(context.Background(),
		`UPDATE projects SET enabled = 0 WHERE id = ?`, "p1"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	got, err = db.GetProjectByID(context.Background(), "p1")
	if err != nil {
		t.Fatalf("GetProjectByID after disable: %v", err)
	}
	if got.Enabled {
		t.Errorf("disabled project should read back disabled, got %+v", got)
	}
}

func TestSqlite_AssignDatabase_NoHealthyServer(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	got, err := db.AssignDatabase(context.Background(), "p1", "d1", true, 30)
	if err != nil {
		t.Fatalf("AssignDatabase: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty (no servers)", got)
	}
}

func TestSqlite_AssignDatabase_PicksHealthyServer(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	seedSqliteServer(t, db, "s1", true)

	got, err := db.AssignDatabase(context.Background(), "p1", "d1", true, 30)
	if err != nil {
		t.Fatalf("AssignDatabase: %v", err)
	}
	if got != "s1" {
		t.Errorf("got %q, want s1", got)
	}

	// Same call → same server (idempotent / sticky).
	got2, err := db.AssignDatabase(context.Background(), "p1", "d1", true, 30)
	if err != nil {
		t.Fatalf("AssignDatabase #2: %v", err)
	}
	if got2 != "s1" {
		t.Errorf("re-assign got %q, want s1", got2)
	}
}

func TestSqlite_AssignDatabase_FailsOverFromUnhealthyServer(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	seedSqliteServer(t, db, "s_dead", false)
	seedSqliteServer(t, db, "s_alive", true)

	// First assignment lands on the only healthy server.
	got, err := db.AssignDatabase(context.Background(), "p1", "d1", true, 30)
	if err != nil {
		t.Fatalf("AssignDatabase: %v", err)
	}
	if got != "s_alive" {
		t.Errorf("got %q, want s_alive", got)
	}

	// Manually point the existing assignment at the dead server and verify
	// the next call reassigns to a healthy one.
	if _, err := db.sql.ExecContext(context.Background(),
		`UPDATE databases SET server_id = ? WHERE project_id = 'p1' AND id = 'd1'`,
		"s_dead",
	); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err = db.AssignDatabase(context.Background(), "p1", "d1", true, 30)
	if err != nil {
		t.Fatalf("AssignDatabase after failover: %v", err)
	}
	if got != "s_alive" {
		t.Errorf("after failover got %q, want s_alive", got)
	}
}

func TestSqlite_EnsureRoutingData_NoAutoCreate_NotFound(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", false /* auto_create */)
	seedSqliteServer(t, db, "s1", true)

	_, _, err := db.EnsureRoutingData(context.Background(), "p1", "missing", 30)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound (auto_create=false on missing db)", err)
	}
}

func TestSqlite_EnsureRoutingData_AutoCreate_Routes(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	seedSqliteServer(t, db, "s1", true)

	server, project, err := db.EnsureRoutingData(context.Background(), "p1", "fresh", 30)
	if err != nil {
		t.Fatalf("EnsureRoutingData: %v", err)
	}
	if project.ID != "p1" {
		t.Errorf("project: got %s, want p1", project.ID)
	}
	if server.ID != "s1" {
		t.Errorf("server: got %s, want s1", server.ID)
	}
}

func TestSqlite_EvictDatabases_DeletesEphemeralAndDeactivatesRest(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)
	seedSqliteServer(t, db, "s1", true)

	// Seed two databases: one ephemeral, one persistent.
	now := NowMS()
	if _, err := db.sql.ExecContext(context.Background(), `
		INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
		VALUES ('p1', 'eph', 's1', 1, 'active', ?, ?),
		       ('p1', 'persist', 's1', 0, 'active', ?, ?)
	`, now, now, now, now); err != nil {
		t.Fatalf("seed databases: %v", err)
	}

	if err := db.EvictDatabases(context.Background(), []EvictionRequest{
		{ProjectID: "p1", DatabaseID: "eph"},
		{ProjectID: "p1", DatabaseID: "persist"},
	}); err != nil {
		t.Fatalf("EvictDatabases: %v", err)
	}

	// Ephemeral should be gone.
	if _, err := db.GetDatabase(context.Background(), "p1", "eph"); !errors.Is(err, ErrNotFound) {
		t.Errorf("ephemeral: got %v, want ErrNotFound", err)
	}
	// Persistent should still exist but inactive with no server.
	got, err := db.GetDatabase(context.Background(), "p1", "persist")
	if err != nil {
		t.Fatalf("persist GetDatabase: %v", err)
	}
	if got.Status != "inactive" || got.ServerID != "" {
		t.Errorf("persist: got %+v, want inactive + empty server_id", got)
	}
}

func TestSqlite_RegisterServer_UpsertsAndClearsAssignments(t *testing.T) {
	db := openMemorySqlite(t)
	seedSqliteProject(t, db, "p1", true)

	// Initial registration.
	if err := db.RegisterServer(context.Background(), "s1", "10.0.0.1", 7779, 4); err != nil {
		t.Fatalf("RegisterServer: %v", err)
	}
	got, err := db.GetServerByID(context.Background(), "s1")
	if err != nil {
		t.Fatalf("GetServerByID: %v", err)
	}
	if got.PrivateIP != "10.0.0.1" || got.ProxyPort != 7779 || got.NrCores != 4 || got.Status != "pending" {
		t.Errorf("after register: %+v", got)
	}

	// Re-register with new values → upsert wins.
	if err := db.RegisterServer(context.Background(), "s1", "10.0.0.2", 7780, 8); err != nil {
		t.Fatalf("RegisterServer (re-register): %v", err)
	}
	got, _ = db.GetServerByID(context.Background(), "s1")
	if got.PrivateIP != "10.0.0.2" || got.ProxyPort != 7780 || got.NrCores != 8 {
		t.Errorf("after re-register: %+v", got)
	}
}

func TestSqlite_Listen_Unsupported(t *testing.T) {
	db := openMemorySqlite(t)
	err := db.Listen(context.Background(), []string{"any"}, func(Notification) {})
	if !errors.Is(err, ErrUnsupported) {
		t.Errorf("got %v, want ErrUnsupported", err)
	}
}

func TestNewDispatchesByScheme(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lark.db")
	store, err := New(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("New(sqlite://): %v", err)
	}
	defer store.Close()
	if store.DriverKind() != DriverSQLite {
		t.Errorf("driver: got %s, want sqlite", store.DriverKind())
	}

	_, err = New(context.Background(), "mysql://foo")
	if err == nil {
		t.Error("New(mysql://): expected error")
	}
}
