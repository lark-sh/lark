package db

import (
	"context"
	"time"
)

// LocalDB is the in-memory mock implementation of [Store]. Used by
// LOCAL_MODE for development and by tests that don't need a real backend.
//
// Returns a single permissive project and a single fake server; never
// persists anything. Anything that would write (heartbeats, evictions,
// metrics, etc.) is a no-op.
type LocalDB struct {
	projectID   string
	backendAddr string
	serverID    string
}

// NewLocalDB creates a new local mock store.
func NewLocalDB(projectID, backendAddr string) *LocalDB {
	return &LocalDB{
		projectID:   projectID,
		backendAddr: backendAddr,
		serverID:    "local-server",
	}
}

func (db *LocalDB) DriverKind() Driver { return DriverLocal }
func (db *LocalDB) Close()             {}

// ---------------------------------------------------------------------------
// Accounts and sessions — not used in LOCAL_MODE (no admin API runs).
// ---------------------------------------------------------------------------

func (db *LocalDB) GetAccountByID(ctx context.Context, id string) (*Account, error) {
	return nil, ErrNotFound
}
func (db *LocalDB) GetAccountByEmail(ctx context.Context, email string) (*Account, error) {
	return nil, ErrNotFound
}
func (db *LocalDB) ListAccounts(ctx context.Context) ([]*Account, error)      { return nil, nil }
func (db *LocalDB) CreateAccount(ctx context.Context, a *Account) error       { return ErrUnsupported }
func (db *LocalDB) DeleteAccount(ctx context.Context, accountID string) error { return ErrUnsupported }
func (db *LocalDB) UpdateAccountPassword(ctx context.Context, accountID, passwordHash string, mustChangePassword bool) error {
	return ErrUnsupported
}
func (db *LocalDB) CountAccounts(ctx context.Context) (int, error) { return 0, nil }

func (db *LocalDB) CreateSession(ctx context.Context, s *Session) error { return ErrUnsupported }
func (db *LocalDB) GetSessionByID(ctx context.Context, id string) (*Session, error) {
	return nil, ErrNotFound
}
func (db *LocalDB) DeleteSession(ctx context.Context, id string) error { return nil }

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

func (db *LocalDB) GetProjectByID(ctx context.Context, projectID string) (*Project, error) {
	return &Project{
		ID:                    db.projectID,
		Name:                  "Local Project",
		SecretKey:             "local-secret-key",
		AdminSecretKey:        "local-admin-secret",
		RulesJSON:             `{"rules":{".read":true,".write":true}}`,
		Ephemeral:             true,
		AutoCreate:            true,
		FirebaseCompatEnabled: true,
		Enabled:               true,
		ConfigVersion:         1,
	}, nil
}

func (db *LocalDB) ListProjects(ctx context.Context) ([]*Project, error) {
	p, _ := db.GetProjectByID(ctx, db.projectID)
	return []*Project{p}, nil
}

func (db *LocalDB) CreateProject(ctx context.Context, p *Project) error { return ErrUnsupported }
func (db *LocalDB) UpdateProject(ctx context.Context, p *Project) (int64, error) {
	return 0, ErrUnsupported
}
func (db *LocalDB) DeleteProject(ctx context.Context, projectID string) error { return ErrUnsupported }
func (db *LocalDB) RegenerateProjectSecret(ctx context.Context, projectID, newSecretKey string) (int64, error) {
	return 0, ErrUnsupported
}

// ---------------------------------------------------------------------------
// Databases — local mode treats every database as ephemeral and routed to
// the single fake server.
// ---------------------------------------------------------------------------

func (db *LocalDB) GetDatabase(ctx context.Context, projectID, id string) (*Database, error) {
	return nil, ErrNotFound
}

func (db *LocalDB) ListDatabasesByProject(ctx context.Context, projectID string) ([]*Database, error) {
	return nil, nil
}

func (db *LocalDB) CreateDatabase(ctx context.Context, projectID, id string, ephemeral bool) error {
	return ErrUnsupported
}

func (db *LocalDB) GetActiveDatabasesByProject(ctx context.Context, projectID string) ([]ActiveDatabaseAssignment, error) {
	return nil, nil
}

func (db *LocalDB) EvictDatabases(ctx context.Context, evictions []EvictionRequest) error {
	return nil
}

// ---------------------------------------------------------------------------
// Servers — a single fake "local-server" exists; everything else is no-op.
// ---------------------------------------------------------------------------

func (db *LocalDB) GetServerByID(ctx context.Context, id string) (*Server, error) {
	if id != db.serverID {
		return nil, ErrNotFound
	}
	return db.localServer(), nil
}

func (db *LocalDB) GetHealthyServers(ctx context.Context, heartbeatTimeout int64) ([]*Server, error) {
	return []*Server{db.localServer()}, nil
}

func (db *LocalDB) GetBestServer(ctx context.Context, heartbeatTimeout int64) (*Server, error) {
	return db.localServer(), nil
}

func (db *LocalDB) CreateServer(ctx context.Context, id, hostname, ipAddress, privateIP string, udpPort, capacity int) (*Server, error) {
	return db.localServer(), nil
}

func (db *LocalDB) UpdateServerHeartbeat(ctx context.Context, id string, dbCount, connCount int) error {
	return nil
}

func (db *LocalDB) IncrementServerDatabaseCount(ctx context.Context, id string) error { return nil }
func (db *LocalDB) DecrementServerDatabaseCount(ctx context.Context, id string) error { return nil }
func (db *LocalDB) SetServerStatus(ctx context.Context, id, status string) error      { return nil }

func (db *LocalDB) GetUnhealthyServers(ctx context.Context, deathTimeout int64) ([]*Server, error) {
	return nil, nil
}

func (db *LocalDB) ClearServerAssignments(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	return nil, nil
}

func (db *LocalDB) ClearServerAssignmentsOnStartup(ctx context.Context, serverID string) ([]DatabaseKey, error) {
	return nil, nil
}

func (db *LocalDB) RegisterServer(ctx context.Context, serverID, privateIP string, proxyPort, nrCores int) error {
	return nil
}

func (db *LocalDB) GetAllServersForDiscovery(ctx context.Context) ([]*Server, error) {
	return []*Server{}, nil
}

func (db *LocalDB) UpdateServerHeartbeatFromProxy(ctx context.Context, serverID string, load, clients, memMB int) error {
	return nil
}

func (db *LocalDB) localServer() *Server {
	return &Server{
		ID:        db.serverID,
		Hostname:  "localhost",
		IPAddress: "127.0.0.1",
		PrivateIP: "127.0.0.1",
		UDPPort:   0,
		ProxyPort: 7779,
		NrCores:   1,
		Status:    "active",
	}
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

func (db *LocalDB) EnsureRoutingData(ctx context.Context, projectID, databaseID string, heartbeatTimeout int64) (*Server, *Project, error) {
	// Validate the database ID even in local mode: it flows to the server and
	// into its on-disk data-dir path, so an unvalidated id (e.g. "../foo") would
	// escape the project's directory. Mirrors the SQLite/Postgres backends.
	if err := ValidateDatabaseID(databaseID); err != nil {
		return nil, nil, err
	}
	project, _ := db.GetProjectByID(ctx, projectID)
	return db.localServer(), project, nil
}

func (db *LocalDB) AssignDatabase(ctx context.Context, projectID, databaseID string, ephemeral bool, heartbeatTimeout int64) (string, error) {
	return db.serverID, nil
}

// ---------------------------------------------------------------------------
// Metrics — no-op in local mode.
// ---------------------------------------------------------------------------

func (db *LocalDB) InsertDatabaseMetricsBatch(ctx context.Context, metrics []*DatabaseMetricsRow) error {
	return nil
}

func (db *LocalDB) InsertDatabaseEvent(ctx context.Context, e *DatabaseEvent) error {
	return nil
}

func (db *LocalDB) GetProjectMetricsRange(ctx context.Context, projectID string, start, end time.Time) ([]*ProjectMetricsRow, error) {
	return nil, nil
}

func (db *LocalDB) ListDatabaseEvents(ctx context.Context, projectID string, limit, offset int) ([]*DatabaseEvent, int, error) {
	return nil, 0, nil
}

// ---------------------------------------------------------------------------
// Notify — unsupported.
// ---------------------------------------------------------------------------

func (db *LocalDB) Listen(ctx context.Context, channels []string, handler func(Notification)) error {
	return ErrUnsupported
}
