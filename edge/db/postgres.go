package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDB is the Postgres-backed implementation of [Store]. Constructed
// via [New] when DATABASE_URL has a postgres:// or postgresql:// scheme.
//
// Methods are grouped (Projects / Databases / Servers / Routing / Metrics /
// Notify) to mirror the layout of sqlite.go — the two files should be
// diff-readable side by side.
type PostgresDB struct {
	pool *pgxpool.Pool

	// directURL is the same connection string as the pool's but with Neon's
	// "-pooler" suffix stripped from the hostname. Used for LISTEN
	// connections, which don't work through PgBouncer in transaction mode
	// (the pooler). Non-Neon URLs are passed through unchanged.
	directURL string
}

// NewPostgres opens a Postgres connection directly, returning the concrete
// type. Production code uses [New] and depends on the [Store] interface
// instead; this is exported for tests that need raw access (e.g. fixture
// inserts via Exec).
func NewPostgres(ctx context.Context, databaseURL string) (*PostgresDB, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	config.MaxConns = 20
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &PostgresDB{pool: pool, directURL: deriveDirectURL(databaseURL)}, nil
}

// deriveDirectURL strips Neon's "-pooler" hostname suffix so LISTEN/NOTIFY
// bypasses PgBouncer (which runs in transaction mode on Neon and silently
// breaks LISTEN). For non-Neon URLs or URLs without "-pooler", returns the
// input unchanged.
func deriveDirectURL(databaseURL string) string {
	u, err := url.Parse(databaseURL)
	if err != nil || u.Host == "" {
		return databaseURL
	}
	host := u.Hostname()
	if !strings.Contains(host, "-pooler.") {
		return databaseURL
	}
	newHost := strings.Replace(host, "-pooler.", ".", 1)
	if port := u.Port(); port != "" {
		u.Host = newHost + ":" + port
	} else {
		u.Host = newHost
	}
	return u.String()
}

func (db *PostgresDB) DriverKind() Driver { return DriverPostgres }

func (db *PostgresDB) Close() {
	db.pool.Close()
}

// Exec executes a query without returning any rows. Used by test helpers
// for fixture setup; production code uses typed methods.
func (db *PostgresDB) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	result, err := db.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

func (db *PostgresDB) GetAccountByID(ctx context.Context, id string) (*Account, error) {
	var a Account
	err := db.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, role, must_change_password, created_at
		FROM accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.Email, &a.PasswordHash, &a.Role, &a.MustChangePassword, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (db *PostgresDB) GetAccountByEmail(ctx context.Context, email string) (*Account, error) {
	var a Account
	err := db.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, role, must_change_password, created_at
		FROM accounts WHERE email = $1
	`, email).Scan(&a.ID, &a.Email, &a.PasswordHash, &a.Role, &a.MustChangePassword, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (db *PostgresDB) CreateAccount(ctx context.Context, a *Account) error {
	if a.CreatedAt == 0 {
		a.CreatedAt = NowMS()
	}
	if a.Role == "" {
		a.Role = "admin"
	}
	_, err := db.pool.Exec(ctx, `
		INSERT INTO accounts (id, email, password_hash, role, must_change_password, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, a.ID, a.Email, a.PasswordHash, a.Role, a.MustChangePassword, a.CreatedAt)
	return err
}

func (db *PostgresDB) UpdateAccountPassword(ctx context.Context, accountID, passwordHash string, mustChangePassword bool) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE accounts SET password_hash = $1, must_change_password = $2 WHERE id = $3
	`, passwordHash, mustChangePassword, accountID)
	return err
}

func (db *PostgresDB) CountAccounts(ctx context.Context) (int, error) {
	var n int
	err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&n)
	return n, err
}

func (db *PostgresDB) ListAccounts(ctx context.Context) ([]*Account, error) {
	rows, err := db.pool.Query(ctx, `
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
		if err := rows.Scan(&a.ID, &a.Email, &a.PasswordHash, &a.Role, &a.MustChangePassword, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (db *PostgresDB) DeleteAccount(ctx context.Context, accountID string) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	return err
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

func (db *PostgresDB) CreateSession(ctx context.Context, s *Session) error {
	if s.CreatedAt == 0 {
		s.CreatedAt = NowMS()
	}
	if s.Kind == "" {
		s.Kind = "dashboard"
	}
	_, err := db.pool.Exec(ctx, `
		INSERT INTO sessions (id, public_id, account_id, kind, name, last_used_at,
		                     created_ip, created_user_agent, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		s.ID, s.PublicID, s.AccountID, s.Kind,
		nullableString(s.Name), nullableInt64(s.LastUsedAt),
		nullableString(s.CreatedIP), nullableString(s.CreatedUserAgent),
		s.ExpiresAt, s.CreatedAt,
	)
	return err
}

func (db *PostgresDB) GetSessionByID(ctx context.Context, id string) (*Session, error) {
	var s Session
	var name, ip, ua *string
	var lastUsed *int64
	err := db.pool.QueryRow(ctx, `
		SELECT id, public_id, account_id, kind, name, last_used_at,
		       created_ip, created_user_agent, expires_at, created_at
		FROM sessions WHERE id = $1
	`, id).Scan(
		&s.ID, &s.PublicID, &s.AccountID, &s.Kind, &name, &lastUsed,
		&ip, &ua, &s.ExpiresAt, &s.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if name != nil {
		s.Name = *name
	}
	if lastUsed != nil {
		s.LastUsedAt = *lastUsed
	}
	if ip != nil {
		s.CreatedIP = *ip
	}
	if ua != nil {
		s.CreatedUserAgent = *ua
	}
	return &s, nil
}

func (db *PostgresDB) DeleteSession(ctx context.Context, id string) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM sessions WHERE id = $1`, id)
	return err
}

// nullableString returns nil for the empty string so INSERTs land NULL
// instead of ” in nullable columns.
func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// nullableInt64 returns nil for 0 so INSERTs land NULL in nullable columns.
func nullableInt64(v int64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

func (db *PostgresDB) GetProjectByID(ctx context.Context, id string) (*Project, error) {
	var p Project
	err := db.pool.QueryRow(ctx, `
		SELECT id, name, secret_key, admin_secret_key, rules_json,
		       ephemeral, auto_create, firebase_compat_enabled,
		       firebase_project_id, enabled,
		       config_version, created_at, updated_at
		FROM projects WHERE id = $1
	`, id).Scan(
		&p.ID, &p.Name, &p.SecretKey, &p.AdminSecretKey, &p.RulesJSON,
		&p.Ephemeral, &p.AutoCreate, &p.FirebaseCompatEnabled,
		&p.FirebaseProjectID, &p.Enabled,
		&p.ConfigVersion, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (db *PostgresDB) ListProjects(ctx context.Context) ([]*Project, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT id, name, secret_key, admin_secret_key, rules_json,
		       ephemeral, auto_create, firebase_compat_enabled,
		       firebase_project_id, enabled,
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
			&p.FirebaseProjectID, &p.Enabled,
			&p.ConfigVersion, &p.CreatedAt, &p.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}

func (db *PostgresDB) CreateProject(ctx context.Context, p *Project) error {
	if p.CreatedAt == 0 {
		p.CreatedAt = NowMS()
	}
	if p.UpdatedAt == 0 {
		p.UpdatedAt = p.CreatedAt
	}
	if p.ConfigVersion == 0 {
		p.ConfigVersion = 1
	}
	// New projects start enabled.
	p.Enabled = true
	_, err := db.pool.Exec(ctx, `
		INSERT INTO projects (id, name, secret_key, admin_secret_key, rules_json,
		                     ephemeral, auth_required, auto_create, firebase_compat_enabled,
		                     firebase_project_id, enabled,
		                     config_version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`,
		p.ID, p.Name, p.SecretKey, p.AdminSecretKey, p.RulesJSON,
		p.Ephemeral, false /* auth_required */, p.AutoCreate, p.FirebaseCompatEnabled,
		p.FirebaseProjectID, p.Enabled,
		p.ConfigVersion, p.CreatedAt, p.UpdatedAt,
	)
	return err
}

func (db *PostgresDB) UpdateProject(ctx context.Context, p *Project) (int64, error) {
	var newVersion int64
	err := db.pool.QueryRow(ctx, `
		UPDATE projects SET
			name = $2,
			secret_key = $3,
			admin_secret_key = $4,
			rules_json = $5,
			ephemeral = $6,
			auto_create = $7,
			firebase_compat_enabled = $8,
			firebase_project_id = $9,
			config_version = config_version + 1,
			updated_at = $10
		WHERE id = $1
		RETURNING config_version
	`,
		p.ID, p.Name, p.SecretKey, p.AdminSecretKey, p.RulesJSON,
		p.Ephemeral, p.AutoCreate, p.FirebaseCompatEnabled,
		p.FirebaseProjectID,
		NowMS(),
	).Scan(&newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	return newVersion, err
}

func (db *PostgresDB) DeleteProject(ctx context.Context, projectID string) error {
	res, err := db.pool.Exec(ctx, `DELETE FROM projects WHERE id = $1`, projectID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (db *PostgresDB) RegenerateProjectSecret(ctx context.Context, projectID, newSecretKey string) (int64, error) {
	var newVersion int64
	err := db.pool.QueryRow(ctx, `
		UPDATE projects
		SET secret_key = $2, config_version = config_version + 1, updated_at = $3
		WHERE id = $1
		RETURNING config_version
	`, projectID, newSecretKey, NowMS()).Scan(&newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	return newVersion, err
}

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

func (db *PostgresDB) GetDatabase(ctx context.Context, projectID, id string) (*Database, error) {
	var d Database
	err := db.pool.QueryRow(ctx, `
		SELECT project_id, id, COALESCE(server_id, ''), ephemeral, status, last_activity, created_at
		FROM databases WHERE project_id = $1 AND id = $2
	`, projectID, id).Scan(
		&d.ProjectID, &d.ID, &d.ServerID, &d.Ephemeral, &d.Status, &d.LastActivity, &d.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (db *PostgresDB) CreateDatabase(ctx context.Context, projectID, id string, ephemeral bool) error {
	now := NowMS()
	_, err := db.pool.Exec(ctx, `
		INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
		VALUES ($1, $2, NULL, $3, 'inactive', $4, $4)
	`, projectID, id, ephemeral, now)
	return err
}

func (db *PostgresDB) ListDatabasesByProject(ctx context.Context, projectID string) ([]*Database, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT project_id, id, COALESCE(server_id, ''), ephemeral, status, last_activity, created_at
		FROM databases WHERE project_id = $1 ORDER BY created_at ASC
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Database
	for rows.Next() {
		var d Database
		if err := rows.Scan(&d.ProjectID, &d.ID, &d.ServerID, &d.Ephemeral, &d.Status, &d.LastActivity, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &d)
	}
	return out, rows.Err()
}

func (db *PostgresDB) GetActiveDatabasesByProject(ctx context.Context, projectID string) ([]ActiveDatabaseAssignment, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT id, server_id
		FROM databases
		WHERE project_id = $1 AND status = 'active' AND server_id IS NOT NULL
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

// EvictDatabases batches eviction in a single CTE-driven query. Ephemeral
// databases are deleted; persistent databases have their server_id cleared
// and status set to 'inactive'. Idempotent.
func (db *PostgresDB) EvictDatabases(ctx context.Context, evictions []EvictionRequest) error {
	if len(evictions) == 0 {
		return nil
	}

	projectIDs := make([]string, len(evictions))
	databaseIDs := make([]string, len(evictions))
	for i, e := range evictions {
		projectIDs[i] = e.ProjectID
		databaseIDs[i] = e.DatabaseID
	}

	_, err := db.pool.Exec(ctx, `
		WITH batch AS (
			SELECT * FROM unnest($1::text[], $2::text[]) AS t(project_id, database_id)
		),
		updated AS (
			UPDATE databases d
			SET server_id = NULL, status = 'inactive'
			FROM batch b
			WHERE d.project_id = b.project_id AND d.id = b.database_id
		)
		DELETE FROM databases d
		USING batch b
		WHERE d.project_id = b.project_id AND d.id = b.database_id AND d.ephemeral = true
	`, projectIDs, databaseIDs)
	return err
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

func (db *PostgresDB) GetServerByID(ctx context.Context, id string) (*Server, error) {
	var s Server
	err := db.pool.QueryRow(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, last_heartbeat,
		       database_count, connection_count, capacity, status
		FROM servers WHERE id = $1
	`, id).Scan(
		&s.ID, &s.Hostname, &s.IPAddress, &s.PrivateIP, &s.UDPPort, &s.LastHeartbeat,
		&s.DatabaseCount, &s.ConnectionCount, &s.Capacity, &s.Status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *PostgresDB) GetHealthyServers(ctx context.Context, heartbeatTimeout int64) ([]*Server, error) {
	cutoff := NowMS() - heartbeatTimeout*1000

	rows, err := db.pool.Query(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, last_heartbeat,
		       database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'active' AND last_heartbeat > $1
		ORDER BY database_count ASC
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPgServers(rows)
}

func (db *PostgresDB) GetBestServer(ctx context.Context, heartbeatTimeout int64) (*Server, error) {
	cutoff := NowMS() - heartbeatTimeout*1000

	var s Server
	err := db.pool.QueryRow(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, last_heartbeat,
		       database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'active'
		  AND last_heartbeat > $1
		  AND database_count < capacity
		ORDER BY database_count ASC
		LIMIT 1
	`, cutoff).Scan(
		&s.ID, &s.Hostname, &s.IPAddress, &s.PrivateIP, &s.UDPPort, &s.LastHeartbeat,
		&s.DatabaseCount, &s.ConnectionCount, &s.Capacity, &s.Status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *PostgresDB) CreateServer(ctx context.Context, id, hostname, ipAddress, privateIP string, udpPort, capacity int) (*Server, error) {
	now := NowMS()

	_, err := db.pool.Exec(ctx, `
		INSERT INTO servers (id, hostname, ip_address, private_ip, udp_port, last_heartbeat,
		                     database_count, connection_count, capacity, status)
		VALUES ($1, $2, $3, $4, $5, $6, 0, 0, $7, 'active')
	`, id, hostname, ipAddress, privateIP, udpPort, now, capacity)
	if err != nil {
		return nil, err
	}

	return &Server{
		ID:            id,
		Hostname:      hostname,
		IPAddress:     ipAddress,
		PrivateIP:     privateIP,
		UDPPort:       udpPort,
		LastHeartbeat: now,
		Capacity:      capacity,
		Status:        "active",
	}, nil
}

func (db *PostgresDB) UpdateServerHeartbeat(ctx context.Context, id string, dbCount, connCount int) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE servers SET last_heartbeat = $1, database_count = $2, connection_count = $3, status = 'active'
		WHERE id = $4
	`, NowMS(), dbCount, connCount, id)
	return err
}

func (db *PostgresDB) IncrementServerDatabaseCount(ctx context.Context, id string) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE servers SET database_count = database_count + 1 WHERE id = $1
	`, id)
	return err
}

func (db *PostgresDB) DecrementServerDatabaseCount(ctx context.Context, id string) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE servers SET database_count = GREATEST(0, database_count - 1) WHERE id = $1
	`, id)
	return err
}

func (db *PostgresDB) SetServerStatus(ctx context.Context, id, status string) error {
	_, err := db.pool.Exec(ctx, `UPDATE servers SET status = $1 WHERE id = $2`, status, id)
	return err
}

func (db *PostgresDB) GetUnhealthyServers(ctx context.Context, deathTimeout int64) ([]*Server, error) {
	cutoff := NowMS() - deathTimeout*1000

	rows, err := db.pool.Query(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, last_heartbeat,
		       database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'active' AND last_heartbeat < $1
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPgServers(rows)
}

// ClearServerAssignments handles a dead server: deletes ephemeral databases,
// marks persistent ones inactive with no server. Idempotent.
func (db *PostgresDB) ClearServerAssignments(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	keys, err := db.fetchKeysForServer(ctx, serverID)
	if err != nil {
		return nil, err
	}

	_, err = db.pool.Exec(ctx, `
		WITH updated AS (
			UPDATE databases
			SET server_id = NULL, status = 'inactive'
			WHERE server_id = $1
		)
		DELETE FROM databases
		WHERE server_id = $1 AND ephemeral = true
	`, serverID)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// ClearServerAssignmentsOnStartup handles a server restart: keeps server_id
// pinned for persistent databases (sticky routing) and deletes ephemeral ones
// (their data is gone after restart anyway). Idempotent.
func (db *PostgresDB) ClearServerAssignmentsOnStartup(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	keys, err := db.fetchKeysForServer(ctx, serverID)
	if err != nil {
		return nil, err
	}

	_, err = db.pool.Exec(ctx, `
		WITH updated AS (
			UPDATE databases
			SET status = 'inactive'
			WHERE server_id = $1 AND ephemeral = false
		)
		DELETE FROM databases
		WHERE server_id = $1 AND ephemeral = true
	`, serverID)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (db *PostgresDB) fetchKeysForServer(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT project_id, id FROM databases WHERE server_id = $1
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

// RegisterServer upserts a server row (called by backends on startup via the
// HTTP API) and clears any stale assignments from a previous run.
func (db *PostgresDB) RegisterServer(ctx context.Context, serverID, privateIP string, proxyPort, nrCores int) error {
	now := NowMS()

	_, err := db.pool.Exec(ctx, `
		INSERT INTO servers (id, hostname, ip_address, private_ip, udp_port, proxy_port, nr_cores,
		                     last_heartbeat, database_count, connection_count, capacity, status)
		VALUES ($1, $1, '', $2, 0, $3, $4, $5, 0, 0, 50000, 'pending')
		ON CONFLICT (id) DO UPDATE SET
			private_ip = $2,
			proxy_port = $3,
			nr_cores = $4,
			last_heartbeat = $5,
			database_count = 0,
			connection_count = 0,
			capacity = 50000,
			status = 'pending'
	`, serverID, privateIP, proxyPort, nrCores, now)
	if err != nil {
		return err
	}

	_, err = db.ClearServerAssignmentsOnStartup(ctx, serverID)
	return err
}

// GetAllServersForDiscovery returns servers that are either pending or have
// been active in the last 5 minutes — the window where the proxy can still
// usefully connect.
func (db *PostgresDB) GetAllServersForDiscovery(ctx context.Context) ([]*Server, error) {
	cutoff := NowMS() - 5*60*1000 // 5 minutes ago

	rows, err := db.pool.Query(ctx, `
		SELECT id, hostname, ip_address, private_ip, udp_port, COALESCE(proxy_port, 0), COALESCE(nr_cores, 1),
		       last_heartbeat, database_count, connection_count, capacity, status
		FROM servers
		WHERE status = 'pending'
		   OR (status = 'active' AND last_heartbeat > $1)
		   OR status = 'draining'
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (db *PostgresDB) UpdateServerHeartbeatFromProxy(ctx context.Context, serverID string, load, clients, memMB int) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE servers
		SET last_heartbeat = $1, connection_count = $2, status = 'active'
		WHERE id = $3
	`, NowMS(), clients, serverID)
	return err
}

// scanPgServers is the shared row → Server decoder used by every
// list-servers query that selects the same column set. Lives here (not in
// models.go) because it's pgx-shaped.
func scanPgServers(rows pgx.Rows) ([]*Server, error) {
	var servers []*Server
	for rows.Next() {
		var s Server
		err := rows.Scan(
			&s.ID, &s.Hostname, &s.IPAddress, &s.PrivateIP, &s.UDPPort, &s.LastHeartbeat,
			&s.DatabaseCount, &s.ConnectionCount, &s.Capacity, &s.Status,
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

// GetConnectData fetches project, current database assignment, and assigned
// server in a single LEFT JOIN. Returns nil sub-fields on ConnectData where
// the database or server doesn't exist (or where the server is unhealthy).
func (db *PostgresDB) GetConnectData(ctx context.Context, projectID, databaseID string, heartbeatTimeout int64) (*ConnectData, error) {
	cutoff := NowMS() - heartbeatTimeout*1000

	var p Project
	var d Database
	var s Server

	// Database and server may not exist; scan into nullable pointers.
	var dbID, dbServerID, dbStatus *string
	var dbEphemeral *bool
	var dbLastActivity, dbCreatedAt *int64

	var serverID, serverHostname, serverIPAddress, serverPrivateIP, serverStatus *string
	var serverUDPPort, serverDBCount, serverConnCount, serverCapacity *int
	var serverLastHeartbeat *int64

	err := db.pool.QueryRow(ctx, `
		SELECT
			p.id, p.name, p.secret_key, p.admin_secret_key, p.rules_json,
			p.ephemeral, p.auto_create, p.firebase_compat_enabled,
			p.firebase_project_id,
			p.config_version, p.created_at, p.updated_at,
			d.id, d.server_id, d.ephemeral, d.status, d.last_activity, d.created_at,
			s.id, s.hostname, s.ip_address, s.private_ip, s.udp_port, s.last_heartbeat,
			s.database_count, s.connection_count, s.capacity, s.status
		FROM projects p
		LEFT JOIN databases d ON d.project_id = p.id AND d.id = $2
		LEFT JOIN servers s ON s.id = d.server_id AND s.status = 'active' AND s.last_heartbeat > $3
		WHERE p.id = $1
	`, projectID, databaseID, cutoff).Scan(
		&p.ID, &p.Name, &p.SecretKey, &p.AdminSecretKey, &p.RulesJSON,
		&p.Ephemeral, &p.AutoCreate, &p.FirebaseCompatEnabled,
		&p.FirebaseProjectID,
		&p.ConfigVersion, &p.CreatedAt, &p.UpdatedAt,
		&dbID, &dbServerID, &dbEphemeral, &dbStatus, &dbLastActivity, &dbCreatedAt,
		&serverID, &serverHostname, &serverIPAddress, &serverPrivateIP, &serverUDPPort, &serverLastHeartbeat,
		&serverDBCount, &serverConnCount, &serverCapacity, &serverStatus,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	result := &ConnectData{Project: &p}

	if dbID != nil {
		d.ProjectID = projectID
		d.ID = *dbID
		if dbServerID != nil {
			d.ServerID = *dbServerID
		}
		if dbEphemeral != nil {
			d.Ephemeral = *dbEphemeral
		}
		if dbStatus != nil {
			d.Status = *dbStatus
		}
		if dbLastActivity != nil {
			d.LastActivity = *dbLastActivity
		}
		if dbCreatedAt != nil {
			d.CreatedAt = *dbCreatedAt
		}
		result.Database = &d
	}

	if serverID != nil {
		s.ID = *serverID
		if serverHostname != nil {
			s.Hostname = *serverHostname
		}
		if serverIPAddress != nil {
			s.IPAddress = *serverIPAddress
		}
		if serverPrivateIP != nil {
			s.PrivateIP = *serverPrivateIP
		}
		if serverUDPPort != nil {
			s.UDPPort = *serverUDPPort
		}
		if serverLastHeartbeat != nil {
			s.LastHeartbeat = *serverLastHeartbeat
		}
		if serverDBCount != nil {
			s.DatabaseCount = *serverDBCount
		}
		if serverConnCount != nil {
			s.ConnectionCount = *serverConnCount
		}
		if serverCapacity != nil {
			s.Capacity = *serverCapacity
		}
		if serverStatus != nil {
			s.Status = *serverStatus
		}
		result.Server = &s
	}

	return result, nil
}

// EnsureRoutingData looks up (or auto-creates) the routing record for a
// (project, database) and returns the final server + project config.
func (db *PostgresDB) EnsureRoutingData(ctx context.Context, projectID, databaseID string, heartbeatTimeout int64) (*Server, *Project, error) {
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

// AssignDatabase delegates to the assign_database PL/pgSQL function. That
// function does the existing-assignment lookup, health check, hash-pick,
// and upsert atomically — see schema.sql for its definition.
func (db *PostgresDB) AssignDatabase(ctx context.Context, projectID, databaseID string, ephemeral bool, heartbeatTimeout int64) (string, error) {
	var serverID *string

	err := db.pool.QueryRow(ctx, `
		SELECT assign_database($1, $2, $3, $4)
	`, projectID, databaseID, ephemeral, heartbeatTimeout*1000).Scan(&serverID)

	if err != nil {
		return "", err
	}
	if serverID == nil {
		return "", nil
	}
	return *serverID, nil
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func (db *PostgresDB) InsertDatabaseMetricsBatch(ctx context.Context, metrics []*DatabaseMetricsRow) error {
	if len(metrics) == 0 {
		return nil
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, m := range metrics {
		_, err := tx.Exec(ctx, `
			INSERT INTO database_metrics (
				ts, project_id, database_id, ccu, peak_ccu, bytes_in, bytes_out,
				writes, reads, events_sent,
				permission_denials, connection_rejections,
				data_size_bytes, p50_latency_us, p99_latency_us
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		`,
			m.Timestamp, m.ProjectID, m.DatabaseID, m.CCU, m.PeakCCU, m.BytesIn, m.BytesOut,
			m.Writes, m.Reads, m.EventsSent,
			m.PermissionDenials, m.ConnectionRejections,
			m.DataSizeBytes, m.P50LatencyUs, m.P99LatencyUs,
		)
		if err != nil {
			return fmt.Errorf("insert database metrics: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// GetProjectMetricsRange rolls up database_metrics to project level: each flush
// timestamp groups the project's databases into one point (counters summed,
// peak_ccu/ccu summed across databases, latency averaged).
func (db *PostgresDB) GetProjectMetricsRange(ctx context.Context, projectID string, start, end time.Time) ([]*ProjectMetricsRow, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT ts, project_id,
		       SUM(ccu)::bigint, SUM(peak_ccu)::bigint,
		       SUM(bytes_in)::bigint, SUM(bytes_out)::bigint,
		       SUM(writes)::bigint, SUM(reads)::bigint, SUM(events_sent)::bigint,
		       SUM(permission_denials)::bigint, SUM(connection_rejections)::bigint,
		       COALESCE(AVG(NULLIF(p50_latency_us, 0)), 0)::float8,
		       COALESCE(AVG(NULLIF(p99_latency_us, 0)), 0)::float8
		FROM database_metrics
		WHERE project_id = $1 AND ts >= $2 AND ts <= $3
		GROUP BY ts, project_id
		ORDER BY ts ASC
	`, projectID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ProjectMetricsRow
	for rows.Next() {
		var m ProjectMetricsRow
		var p50, p99 float64
		err := rows.Scan(
			&m.Timestamp, &m.ProjectID, &m.CCU, &m.PeakCCU, &m.BytesIn, &m.BytesOut,
			&m.Writes, &m.Reads, &m.EventsSent, &m.PermissionDenials, &m.ConnectionRejections,
			&p50, &p99,
		)
		if err != nil {
			return nil, err
		}
		m.P50LatencyUs = int(p50)
		m.P99LatencyUs = int(p99)
		out = append(out, &m)
	}
	return out, rows.Err()
}

func (db *PostgresDB) ListDatabaseEvents(ctx context.Context, projectID string, limit, offset int) ([]*DatabaseEvent, int, error) {
	var total int
	if err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM database_events WHERE project_id = $1
	`, projectID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := db.pool.Query(ctx, `
		SELECT id, ts, project_id, database_id, event_type, message, COALESCE(details::text, '')
		FROM database_events
		WHERE project_id = $1
		ORDER BY ts DESC
		LIMIT $2 OFFSET $3
	`, projectID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var events []*DatabaseEvent
	for rows.Next() {
		var e DatabaseEvent
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.ProjectID, &e.DatabaseID, &e.EventType, &e.Message, &e.Details); err != nil {
			return nil, 0, err
		}
		events = append(events, &e)
	}
	return events, total, rows.Err()
}

func (db *PostgresDB) InsertDatabaseEvent(ctx context.Context, e *DatabaseEvent) error {
	// Empty details → NULL (avoids inserting an invalid empty JSON string).
	var details interface{}
	if e.Details != "" {
		details = e.Details
	}

	_, err := db.pool.Exec(ctx, `
		INSERT INTO database_events (ts, project_id, database_id, event_type, message, details)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, e.Timestamp, e.ProjectID, e.DatabaseID, e.EventType, e.Message, details)
	if err != nil {
		return fmt.Errorf("insert database event: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Notify — Postgres-only. Opens a dedicated direct connection (bypassing
// PgBouncer) and pumps notifications into the handler until the context is
// cancelled or the connection errors.
// ---------------------------------------------------------------------------

// Listen opens a dedicated Postgres connection (bypassing the pool), issues
// LISTEN for each of the given channels, and invokes handler for every
// notification until the context is cancelled or the connection errors.
//
// Uses the "direct" URL (Neon's non-pooled endpoint when applicable) because
// PgBouncer in transaction mode silently breaks LISTEN/NOTIFY — the LISTEN
// registers on some backend connection that gets immediately recycled, so no
// session ever receives the notifications.
//
// Channel names are interpolated into the LISTEN statement as SQL
// identifiers (pgx does not support parameters for LISTEN).
func (db *PostgresDB) Listen(ctx context.Context, channels []string, handler func(Notification)) error {
	conn, err := pgx.Connect(ctx, db.directURL)
	if err != nil {
		return fmt.Errorf("connect (direct): %w", err)
	}
	defer conn.Close(context.Background())

	for _, ch := range channels {
		ident := pgx.Identifier{ch}.Sanitize()
		if _, err := conn.Exec(ctx, "LISTEN "+ident); err != nil {
			return fmt.Errorf("listen %s: %w", ch, err)
		}
	}

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		handler(Notification{Channel: n.Channel, Payload: n.Payload})
	}
}
