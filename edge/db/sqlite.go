package db

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed sqlite_schema.sql
var sqliteSchema string

// SqliteDB is the single-file SQLite-backed implementation of [Store].
// Zero-config: the database file is created on first open and the embedded
// schema is applied automatically. Constructed via [New] for a "sqlite://"
// or "file:" URL.
//
// Notes on dialect choices vs. the Postgres impl:
//
//   - Placeholders are "?" (SQLite-native).
//   - AssignDatabase is implemented in Go, in a transaction, since SQLite
//     has no stored procedures.
//   - Listen returns [ErrUnsupported] — SQLite has no NOTIFY/LISTEN.
type SqliteDB struct {
	sql *sql.DB
}

// NewSqlite opens (or creates) a SQLite database file and applies the
// embedded schema if it hasn't been applied yet. Use [New] from production
// code; this is exported for tests that want explicit driver choice.
func NewSqlite(ctx context.Context, databaseURL string) (*SqliteDB, error) {
	dsn, err := sqliteDSN(databaseURL)
	if err != nil {
		return nil, err
	}
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite serializes writes at the file level; a single writer is the
	// sane default. WAL mode (set below) lets readers proceed concurrently.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := conn.ExecContext(ctx, pragma); err != nil {
			conn.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if _, err := conn.ExecContext(ctx, sqliteSchema); err != nil {
		conn.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &SqliteDB{sql: conn}, nil
}

// sqliteDSN turns "sqlite://path/to/file.db" or "file:..." into a DSN the
// SQLite driver understands.
func sqliteDSN(databaseURL string) (string, error) {
	if strings.HasPrefix(databaseURL, "file:") {
		return databaseURL, nil
	}
	const prefix = "sqlite://"
	if !strings.HasPrefix(databaseURL, prefix) {
		return "", fmt.Errorf("invalid sqlite URL: %q", databaseURL)
	}
	return strings.TrimPrefix(databaseURL, prefix), nil
}

func (db *SqliteDB) DriverKind() Driver { return DriverSQLite }

func (db *SqliteDB) Close() {
	db.sql.Close()
}

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

func (db *SqliteDB) GetAccountByID(ctx context.Context, id string) (*Account, error) {
	return scanAccount(db.sql.QueryRowContext(ctx, `
		SELECT id, email, password_hash, role, must_change_password, created_at
		FROM accounts WHERE id = ?
	`, id))
}

func (db *SqliteDB) GetAccountByEmail(ctx context.Context, email string) (*Account, error) {
	return scanAccount(db.sql.QueryRowContext(ctx, `
		SELECT id, email, password_hash, role, must_change_password, created_at
		FROM accounts WHERE email = ?
	`, email))
}

func (db *SqliteDB) CreateAccount(ctx context.Context, a *Account) error {
	if a.CreatedAt == 0 {
		a.CreatedAt = NowMS()
	}
	if a.Role == "" {
		a.Role = "admin"
	}
	mustChange := 0
	if a.MustChangePassword {
		mustChange = 1
	}
	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO accounts (id, email, password_hash, role, must_change_password, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, a.ID, a.Email, a.PasswordHash, a.Role, mustChange, a.CreatedAt)
	return err
}

func (db *SqliteDB) UpdateAccountPassword(ctx context.Context, accountID, passwordHash string, mustChangePassword bool) error {
	mustChange := 0
	if mustChangePassword {
		mustChange = 1
	}
	_, err := db.sql.ExecContext(ctx, `
		UPDATE accounts SET password_hash = ?, must_change_password = ? WHERE id = ?
	`, passwordHash, mustChange, accountID)
	return err
}

func (db *SqliteDB) CountAccounts(ctx context.Context) (int, error) {
	var n int
	err := db.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&n)
	return n, err
}

func (db *SqliteDB) ListAccounts(ctx context.Context) ([]*Account, error) {
	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, email, password_hash, role, must_change_password, created_at
		FROM accounts ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		var mustChange int
		if err := rows.Scan(&a.ID, &a.Email, &a.PasswordHash, &a.Role, &mustChange, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.MustChangePassword = mustChange != 0
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (db *SqliteDB) DeleteAccount(ctx context.Context, accountID string) error {
	_, err := db.sql.ExecContext(ctx, `DELETE FROM accounts WHERE id = ?`, accountID)
	return err
}

func scanAccount(row *sql.Row) (*Account, error) {
	var a Account
	var mustChange int
	err := row.Scan(&a.ID, &a.Email, &a.PasswordHash, &a.Role, &mustChange, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	a.MustChangePassword = mustChange != 0
	return &a, nil
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

func (db *SqliteDB) CreateSession(ctx context.Context, s *Session) error {
	if s.CreatedAt == 0 {
		s.CreatedAt = NowMS()
	}
	if s.Kind == "" {
		s.Kind = "dashboard"
	}
	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO sessions (id, public_id, account_id, kind, name, last_used_at,
		                     created_ip, created_user_agent, expires_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		s.ID, s.PublicID, s.AccountID, s.Kind,
		nullableString(s.Name), nullableInt64(s.LastUsedAt),
		nullableString(s.CreatedIP), nullableString(s.CreatedUserAgent),
		s.ExpiresAt, s.CreatedAt,
	)
	return err
}

func (db *SqliteDB) GetSessionByID(ctx context.Context, id string) (*Session, error) {
	var s Session
	var name, ip, ua sql.NullString
	var lastUsed sql.NullInt64
	err := db.sql.QueryRowContext(ctx, `
		SELECT id, public_id, account_id, kind, name, last_used_at,
		       created_ip, created_user_agent, expires_at, created_at
		FROM sessions WHERE id = ?
	`, id).Scan(
		&s.ID, &s.PublicID, &s.AccountID, &s.Kind, &name, &lastUsed,
		&ip, &ua, &s.ExpiresAt, &s.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	s.Name = name.String
	s.LastUsedAt = lastUsed.Int64
	s.CreatedIP = ip.String
	s.CreatedUserAgent = ua.String
	return &s, nil
}

func (db *SqliteDB) DeleteSession(ctx context.Context, id string) error {
	_, err := db.sql.ExecContext(ctx, `DELETE FROM sessions WHERE id = ?`, id)
	return err
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

func (db *SqliteDB) GetProjectByID(ctx context.Context, id string) (*Project, error) {
	var p Project
	err := db.sql.QueryRowContext(ctx, `
		SELECT id, name, secret_key, admin_secret_key, rules_json,
		       ephemeral, auto_create, firebase_compat_enabled,
		       firebase_project_id,
		       config_version, created_at, updated_at
		FROM projects WHERE id = ?
	`, id).Scan(
		&p.ID, &p.Name, &p.SecretKey, &p.AdminSecretKey, &p.RulesJSON,
		&p.Ephemeral, &p.AutoCreate, &p.FirebaseCompatEnabled,
		&p.FirebaseProjectID,
		&p.ConfigVersion, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (db *SqliteDB) ListProjects(ctx context.Context) ([]*Project, error) {
	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, name, secret_key, admin_secret_key, rules_json,
		       ephemeral, auto_create, firebase_compat_enabled,
		       firebase_project_id,
		       config_version, created_at, updated_at
		FROM projects ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Project
	for rows.Next() {
		var p Project
		err := rows.Scan(
			&p.ID, &p.Name, &p.SecretKey, &p.AdminSecretKey, &p.RulesJSON,
			&p.Ephemeral, &p.AutoCreate, &p.FirebaseCompatEnabled,
			&p.FirebaseProjectID,
			&p.ConfigVersion, &p.CreatedAt, &p.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}

func (db *SqliteDB) CreateProject(ctx context.Context, p *Project) error {
	if p.CreatedAt == 0 {
		p.CreatedAt = NowMS()
	}
	if p.UpdatedAt == 0 {
		p.UpdatedAt = p.CreatedAt
	}
	if p.ConfigVersion == 0 {
		p.ConfigVersion = 1
	}
	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json,
		                     ephemeral, auth_required, auto_create, firebase_compat_enabled,
		                     firebase_project_id,
		                     config_version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		p.ID, p.Name, p.SecretKey, p.AdminSecretKey, p.RulesJSON,
		boolInt(p.Ephemeral), 0 /* auth_required */, boolInt(p.AutoCreate), boolInt(p.FirebaseCompatEnabled),
		p.FirebaseProjectID,
		p.ConfigVersion, p.CreatedAt, p.UpdatedAt,
	)
	return err
}

func (db *SqliteDB) UpdateProject(ctx context.Context, p *Project) (int64, error) {
	var newVersion int64
	err := db.sql.QueryRowContext(ctx, `
		UPDATE projects SET
			name = ?,
			secret_key = ?,
			admin_secret_key = ?,
			rules_json = ?,
			ephemeral = ?,
			auto_create = ?,
			firebase_compat_enabled = ?,
			firebase_project_id = ?,
			config_version = config_version + 1,
			updated_at = ?
		WHERE id = ?
		RETURNING config_version
	`,
		p.Name, p.SecretKey, p.AdminSecretKey, p.RulesJSON,
		boolInt(p.Ephemeral), boolInt(p.AutoCreate), boolInt(p.FirebaseCompatEnabled),
		p.FirebaseProjectID,
		NowMS(),
		p.ID,
	).Scan(&newVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return newVersion, err
}

func (db *SqliteDB) DeleteProject(ctx context.Context, projectID string) error {
	res, err := db.sql.ExecContext(ctx, `DELETE FROM projects WHERE id = ?`, projectID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (db *SqliteDB) RegenerateProjectSecret(ctx context.Context, projectID, newSecretKey string) (int64, error) {
	var newVersion int64
	err := db.sql.QueryRowContext(ctx, `
		UPDATE projects
		SET secret_key = ?, config_version = config_version + 1, updated_at = ?
		WHERE id = ?
		RETURNING config_version
	`, newSecretKey, NowMS(), projectID).Scan(&newVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return newVersion, err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

func (db *SqliteDB) GetDatabase(ctx context.Context, projectID, id string) (*Database, error) {
	var d Database
	var serverID sql.NullString
	err := db.sql.QueryRowContext(ctx, `
		SELECT project_id, id, COALESCE(server_id, ''), ephemeral, status, last_activity, created_at
		FROM databases WHERE project_id = ? AND id = ?
	`, projectID, id).Scan(
		&d.ProjectID, &d.ID, &serverID, &d.Ephemeral, &d.Status, &d.LastActivity, &d.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	d.ServerID = serverID.String
	return &d, nil
}

func (db *SqliteDB) CreateDatabase(ctx context.Context, projectID, id string, ephemeral bool) error {
	now := NowMS()
	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
		VALUES (?, ?, NULL, ?, 'inactive', ?, ?)
	`, projectID, id, boolInt(ephemeral), now, now)
	return err
}

func (db *SqliteDB) ListDatabasesByProject(ctx context.Context, projectID string) ([]*Database, error) {
	rows, err := db.sql.QueryContext(ctx, `
		SELECT project_id, id, COALESCE(server_id, ''), ephemeral, status, last_activity, created_at
		FROM databases WHERE project_id = ? ORDER BY created_at ASC
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Database
	for rows.Next() {
		var d Database
		var ephemeral int
		if err := rows.Scan(&d.ProjectID, &d.ID, &d.ServerID, &ephemeral, &d.Status, &d.LastActivity, &d.CreatedAt); err != nil {
			return nil, err
		}
		d.Ephemeral = ephemeral != 0
		out = append(out, &d)
	}
	return out, rows.Err()
}

func (db *SqliteDB) GetActiveDatabasesByProject(ctx context.Context, projectID string) ([]ActiveDatabaseAssignment, error) {
	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, server_id
		FROM databases
		WHERE project_id = ? AND status = 'active' AND server_id IS NOT NULL
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ActiveDatabaseAssignment
	for rows.Next() {
		var a ActiveDatabaseAssignment
		if err := rows.Scan(&a.DatabaseID, &a.ServerID); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

// EvictDatabases — Postgres can do this in one query with unnest of two
// arrays. SQLite has no array type, so we issue the per-row operations in a
// single transaction instead. Same end state, idempotent either way.
func (db *SqliteDB) EvictDatabases(ctx context.Context, evictions []EvictionRequest) error {
	if len(evictions) == 0 {
		return nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	updateStmt, err := tx.PrepareContext(ctx, `
		UPDATE databases SET server_id = NULL, status = 'inactive'
		WHERE project_id = ? AND id = ?
	`)
	if err != nil {
		return err
	}
	defer updateStmt.Close()

	deleteStmt, err := tx.PrepareContext(ctx, `
		DELETE FROM databases
		WHERE project_id = ? AND id = ? AND ephemeral = 1
	`)
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	for _, e := range evictions {
		if _, err := updateStmt.ExecContext(ctx, e.ProjectID, e.DatabaseID); err != nil {
			return err
		}
		if _, err := deleteStmt.ExecContext(ctx, e.ProjectID, e.DatabaseID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

func (db *SqliteDB) GetServerByID(ctx context.Context, id string) (*Server, error) {
	var s Server
	err := db.sql.QueryRowContext(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, proxy_port, nr_cores,
		       last_heartbeat, database_count, connection_count, capacity, status
		FROM servers WHERE id = ?
	`, id).Scan(
		&s.ID, &s.Hostname, &s.IPAddress, &s.PrivateIP, &s.UDPPort, &s.ProxyPort, &s.NrCores,
		&s.LastHeartbeat, &s.DatabaseCount, &s.ConnectionCount, &s.Capacity, &s.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *SqliteDB) GetHealthyServers(ctx context.Context, heartbeatTimeout int64) ([]*Server, error) {
	cutoff := NowMS() - heartbeatTimeout*1000
	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, proxy_port, nr_cores,
		       last_heartbeat, database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'active' AND last_heartbeat > ?
		ORDER BY database_count ASC
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanServers(rows)
}

func (db *SqliteDB) GetBestServer(ctx context.Context, heartbeatTimeout int64) (*Server, error) {
	cutoff := NowMS() - heartbeatTimeout*1000
	var s Server
	err := db.sql.QueryRowContext(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, proxy_port, nr_cores,
		       last_heartbeat, database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'active'
		  AND last_heartbeat > ?
		  AND database_count < capacity
		ORDER BY database_count ASC
		LIMIT 1
	`, cutoff).Scan(
		&s.ID, &s.Hostname, &s.IPAddress, &s.PrivateIP, &s.UDPPort, &s.ProxyPort, &s.NrCores,
		&s.LastHeartbeat, &s.DatabaseCount, &s.ConnectionCount, &s.Capacity, &s.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *SqliteDB) CreateServer(ctx context.Context, id, hostname, ipAddress, privateIP string, udpPort, capacity int) (*Server, error) {
	now := NowMS()
	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO servers (id, hostname, ip_address, private_ip, udp_port, last_heartbeat,
		                     database_count, connection_count, capacity, status)
		VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, 'active')
	`, id, hostname, ipAddress, privateIP, udpPort, now, capacity)
	if err != nil {
		return nil, err
	}
	return &Server{
		ID: id, Hostname: hostname, IPAddress: ipAddress, PrivateIP: privateIP,
		UDPPort: udpPort, LastHeartbeat: now, Capacity: capacity, Status: "active",
	}, nil
}

func (db *SqliteDB) UpdateServerHeartbeat(ctx context.Context, id string, dbCount, connCount int) error {
	_, err := db.sql.ExecContext(ctx, `
		UPDATE servers SET last_heartbeat = ?, database_count = ?, connection_count = ?, status = 'active'
		WHERE id = ?
	`, NowMS(), dbCount, connCount, id)
	return err
}

func (db *SqliteDB) IncrementServerDatabaseCount(ctx context.Context, id string) error {
	_, err := db.sql.ExecContext(ctx, `
		UPDATE servers SET database_count = database_count + 1 WHERE id = ?
	`, id)
	return err
}

func (db *SqliteDB) DecrementServerDatabaseCount(ctx context.Context, id string) error {
	_, err := db.sql.ExecContext(ctx, `
		UPDATE servers SET database_count = MAX(0, database_count - 1) WHERE id = ?
	`, id)
	return err
}

func (db *SqliteDB) SetServerStatus(ctx context.Context, id, status string) error {
	_, err := db.sql.ExecContext(ctx, `UPDATE servers SET status = ? WHERE id = ?`, status, id)
	return err
}

func (db *SqliteDB) GetUnhealthyServers(ctx context.Context, deathTimeout int64) ([]*Server, error) {
	cutoff := NowMS() - deathTimeout*1000
	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, proxy_port, nr_cores,
		       last_heartbeat, database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'active' AND last_heartbeat < ?
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanServers(rows)
}

// ClearServerAssignments deletes ephemeral databases on a dead server and
// marks persistent ones inactive. Two-statement transaction since SQLite
// can't combine the UPDATE-then-DELETE in a single statement.
func (db *SqliteDB) ClearServerAssignments(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	keys, err := db.fetchKeysForServer(ctx, serverID)
	if err != nil {
		return nil, err
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		UPDATE databases
		SET server_id = NULL, status = 'inactive'
		WHERE server_id = ?
	`, serverID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM databases
		WHERE ephemeral = 1 AND (server_id IS NULL OR server_id = ?)
	`, serverID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

// ClearServerAssignmentsOnStartup keeps the server_id pinned for persistent
// databases (so they re-bind to the same backend after restart) and deletes
// ephemeral ones.
func (db *SqliteDB) ClearServerAssignmentsOnStartup(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	keys, err := db.fetchKeysForServer(ctx, serverID)
	if err != nil {
		return nil, err
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		UPDATE databases
		SET status = 'inactive'
		WHERE server_id = ? AND ephemeral = 0
	`, serverID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM databases
		WHERE server_id = ? AND ephemeral = 1
	`, serverID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (db *SqliteDB) fetchKeysForServer(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	rows, err := db.sql.QueryContext(ctx, `
		SELECT project_id, id FROM databases WHERE server_id = ?
	`, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []DatabaseKey
	for rows.Next() {
		var k DatabaseKey
		if err := rows.Scan(&k.ProjectID, &k.ID); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (db *SqliteDB) RegisterServer(ctx context.Context, serverID, privateIP string, proxyPort, nrCores int) error {
	now := NowMS()

	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO servers (id, hostname, ip_address, private_ip, udp_port, proxy_port, nr_cores,
		                     last_heartbeat, database_count, connection_count, capacity, status)
		VALUES (?, ?, '', ?, 0, ?, ?, ?, 0, 0, 50000, 'pending')
		ON CONFLICT(id) DO UPDATE SET
			private_ip = excluded.private_ip,
			proxy_port = excluded.proxy_port,
			nr_cores = excluded.nr_cores,
			last_heartbeat = excluded.last_heartbeat,
			database_count = 0,
			connection_count = 0,
			capacity = 50000,
			status = 'pending'
	`, serverID, serverID, privateIP, proxyPort, nrCores, now)
	if err != nil {
		return err
	}

	_, err = db.ClearServerAssignmentsOnStartup(ctx, serverID)
	return err
}

func (db *SqliteDB) GetAllServersForDiscovery(ctx context.Context) ([]*Server, error) {
	cutoff := NowMS() - 5*60*1000 // 5 minutes ago

	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, COALESCE(proxy_port, 0), COALESCE(nr_cores, 1),
		       last_heartbeat, database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'pending'
		   OR (status = 'active' AND last_heartbeat > ?)
		   OR status = 'draining'
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanServers(rows)
}

func (db *SqliteDB) UpdateServerHeartbeatFromProxy(ctx context.Context, serverID string, load, clients, memMB int) error {
	_, err := db.sql.ExecContext(ctx, `
		UPDATE servers
		SET last_heartbeat = ?, connection_count = ?, status = 'active'
		WHERE id = ?
	`, NowMS(), clients, serverID)
	return err
}

// scanServers is a small helper shared by every list-servers query.
func scanServers(rows *sql.Rows) ([]*Server, error) {
	var servers []*Server
	for rows.Next() {
		var s Server
		err := rows.Scan(
			&s.ID, &s.Hostname, &s.IPAddress, &s.PrivateIP, &s.UDPPort, &s.ProxyPort, &s.NrCores,
			&s.LastHeartbeat, &s.DatabaseCount, &s.ConnectionCount, &s.Capacity, &s.Status,
		)
		if err != nil {
			return nil, err
		}
		servers = append(servers, &s)
	}
	return servers, rows.Err()
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

func (db *SqliteDB) EnsureRoutingData(ctx context.Context, projectID, databaseID string, heartbeatTimeout int64) (*Server, *Project, error) {
	project, err := db.GetProjectByID(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}

	existingDB, err := db.GetDatabase(ctx, projectID, databaseID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, nil, err
	}

	if existingDB == nil && !project.AutoCreate {
		return nil, nil, ErrNotFound
	}

	if existingDB == nil {
		if err := ValidateDatabaseID(databaseID); err != nil {
			return nil, nil, err
		}
	}

	serverID, err := db.AssignDatabase(ctx, projectID, databaseID, project.Ephemeral, heartbeatTimeout)
	if err != nil {
		return nil, nil, err
	}

	if serverID == "" {
		return nil, nil, ErrNoServersAvailable
	}

	server, err := db.GetServerByID(ctx, serverID)
	if err != nil {
		return nil, nil, err
	}

	return server, project, nil
}

// AssignDatabase picks (or keeps) a healthy server for a (project_id,
// database_id) and persists the assignment. Runs inside a transaction so a
// concurrent assigner sees the final state.
//
// Hash function: FNV-1a over "project_id/database_id". Deterministic per
// key, so the same database lands on the same server as long as the set of
// healthy servers doesn't change.
func (db *SqliteDB) AssignDatabase(ctx context.Context, projectID, databaseID string, ephemeral bool, heartbeatTimeout int64) (string, error) {
	cutoff := NowMS() - heartbeatTimeout*1000

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	// 1. Fast path: existing assignment to a still-healthy server.
	var existingServer sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT server_id FROM databases WHERE project_id = ? AND id = ?
	`, projectID, databaseID).Scan(&existingServer)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	if existingServer.Valid && existingServer.String != "" {
		var healthy int
		err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM servers
			WHERE id = ? AND status = 'active' AND last_heartbeat > ?
		`, existingServer.String, cutoff).Scan(&healthy)
		if err != nil {
			return "", err
		}
		if healthy > 0 {
			if _, err := tx.ExecContext(ctx, `
				UPDATE databases SET status = 'active', last_activity = ?
				WHERE project_id = ? AND id = ? AND status = 'inactive'
			`, NowMS(), projectID, databaseID); err != nil {
				return "", err
			}
			if err := tx.Commit(); err != nil {
				return "", err
			}
			return existingServer.String, nil
		}
		// Stale assignment — clear it so we can repick.
		if _, err := tx.ExecContext(ctx, `
			UPDATE databases SET server_id = NULL
			WHERE project_id = ? AND id = ?
		`, projectID, databaseID); err != nil {
			return "", err
		}
	}

	// 2. Pick a healthy server by deterministic hash.
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM servers
		WHERE status = 'active' AND last_heartbeat > ?
		ORDER BY id
	`, cutoff)
	if err != nil {
		return "", err
	}
	var healthyIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return "", err
		}
		healthyIDs = append(healthyIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(healthyIDs) == 0 {
		// No healthy servers — return empty, caller surfaces ErrNoServersAvailable.
		if err := tx.Commit(); err != nil {
			return "", err
		}
		return "", nil
	}
	pick := healthyIDs[hashRoute(projectID, databaseID)%uint32(len(healthyIDs))]

	// 3. Upsert the assignment. ON CONFLICT preserves any concurrent winner.
	now := NowMS()
	ephemeralInt := 0
	if ephemeral {
		ephemeralInt = 1
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
		VALUES (?, ?, ?, ?, 'active', ?, ?)
		ON CONFLICT(project_id, id) DO UPDATE SET
			server_id = COALESCE(databases.server_id, excluded.server_id),
			status = CASE WHEN databases.server_id IS NULL THEN 'active' ELSE databases.status END,
			last_activity = CASE WHEN databases.server_id IS NULL THEN excluded.last_activity ELSE databases.last_activity END
	`, projectID, databaseID, pick, ephemeralInt, now, now); err != nil {
		return "", err
	}

	// 4. Read back the (possibly raced) final assignment.
	var final sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT server_id FROM databases WHERE project_id = ? AND id = ?
	`, projectID, databaseID).Scan(&final); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	if !final.Valid {
		return "", nil
	}
	return final.String, nil
}

func hashRoute(projectID, databaseID string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(projectID))
	h.Write([]byte{'/'})
	h.Write([]byte(databaseID))
	return h.Sum32()
}

// ---------------------------------------------------------------------------
// Metrics — same data shape as the Postgres impl, with timestamps stored as
// Unix-ms INTEGER columns (vs Postgres' TIMESTAMPTZ).
// ---------------------------------------------------------------------------

func (db *SqliteDB) InsertDatabaseMetricsBatch(ctx context.Context, metrics []*DatabaseMetricsRow) error {
	if len(metrics) == 0 {
		return nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO database_metrics (
			ts, project_id, database_id, ccu, peak_ccu, bytes_in, bytes_out,
			writes, reads, events_sent,
			permission_denials, connection_rejections,
			data_size_bytes, p50_latency_us, p99_latency_us
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range metrics {
		_, err := stmt.ExecContext(ctx,
			m.Timestamp.UnixMilli(), m.ProjectID, m.DatabaseID, m.CCU, m.PeakCCU, m.BytesIn, m.BytesOut,
			m.Writes, m.Reads, m.EventsSent,
			m.PermissionDenials, m.ConnectionRejections,
			m.DataSizeBytes, m.P50LatencyUs, m.P99LatencyUs,
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) InsertDatabaseEvent(ctx context.Context, e *DatabaseEvent) error {
	ts := e.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	var details interface{}
	if e.Details != "" {
		details = e.Details
	}
	_, err := db.sql.ExecContext(ctx, `
		INSERT INTO database_events (ts, project_id, database_id, event_type, message, details)
		VALUES (?, ?, ?, ?, ?, ?)
	`, ts.UnixMilli(), e.ProjectID, e.DatabaseID, e.EventType, e.Message, details)
	return err
}

// GetProjectMetricsRange rolls up database_metrics to project level: each flush
// timestamp groups the project's databases into one point (counters summed,
// peak_ccu/ccu summed across databases, latency averaged).
func (db *SqliteDB) GetProjectMetricsRange(ctx context.Context, projectID string, start, end time.Time) ([]*ProjectMetricsRow, error) {
	rows, err := db.sql.QueryContext(ctx, `
		SELECT ts, project_id,
		       SUM(ccu), SUM(peak_ccu), SUM(bytes_in), SUM(bytes_out),
		       SUM(writes), SUM(reads), SUM(events_sent),
		       SUM(permission_denials), SUM(connection_rejections),
		       COALESCE(AVG(NULLIF(p50_latency_us, 0)), 0),
		       COALESCE(AVG(NULLIF(p99_latency_us, 0)), 0)
		FROM database_metrics
		WHERE project_id = ? AND ts >= ? AND ts <= ?
		GROUP BY ts, project_id
		ORDER BY ts ASC
	`, projectID, start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ProjectMetricsRow
	for rows.Next() {
		var m ProjectMetricsRow
		var ts int64
		var p50, p99 float64
		err := rows.Scan(
			&ts, &m.ProjectID, &m.CCU, &m.PeakCCU, &m.BytesIn, &m.BytesOut,
			&m.Writes, &m.Reads, &m.EventsSent, &m.PermissionDenials, &m.ConnectionRejections,
			&p50, &p99,
		)
		if err != nil {
			return nil, err
		}
		m.Timestamp = time.UnixMilli(ts)
		m.P50LatencyUs = int(p50)
		m.P99LatencyUs = int(p99)
		out = append(out, &m)
	}
	return out, rows.Err()
}

func (db *SqliteDB) ListDatabaseEvents(ctx context.Context, projectID string, limit, offset int) ([]*DatabaseEvent, int, error) {
	var total int
	if err := db.sql.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM database_events WHERE project_id = ?
	`, projectID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := db.sql.QueryContext(ctx, `
		SELECT id, ts, project_id, database_id, event_type, message, COALESCE(details, '')
		FROM database_events
		WHERE project_id = ?
		ORDER BY ts DESC
		LIMIT ? OFFSET ?
	`, projectID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var events []*DatabaseEvent
	for rows.Next() {
		var e DatabaseEvent
		var ts int64
		if err := rows.Scan(&e.ID, &ts, &e.ProjectID, &e.DatabaseID, &e.EventType, &e.Message, &e.Details); err != nil {
			return nil, 0, err
		}
		e.Timestamp = time.UnixMilli(ts)
		events = append(events, &e)
	}
	return events, total, rows.Err()
}

// ---------------------------------------------------------------------------
// Notify — Postgres-only. Admin endpoints invoke the notify handler
// in-process; SQLite has no NOTIFY/LISTEN to subscribe to.
// ---------------------------------------------------------------------------

func (db *SqliteDB) Listen(ctx context.Context, channels []string, handler func(Notification)) error {
	return ErrUnsupported
}
