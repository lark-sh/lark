package db

import (
	"context"
	"time"
)

// Store is the database-agnostic surface every caller outside this package
// depends on. Each backend (Postgres, SQLite, Local) implements it.
//
// Method ordering mirrors the file each impl lives in: projects, then
// databases, then servers, then routing, then metrics, then notify.
type Store interface {
	// Lifecycle.
	Close()
	DriverKind() Driver

	// Accounts (dashboard users).
	GetAccountByID(ctx context.Context, id string) (*Account, error)
	GetAccountByEmail(ctx context.Context, email string) (*Account, error)
	ListAccounts(ctx context.Context) ([]*Account, error)
	CreateAccount(ctx context.Context, a *Account) error
	UpdateAccountPassword(ctx context.Context, accountID, passwordHash string, mustChangePassword bool) error
	DeleteAccount(ctx context.Context, accountID string) error
	CountAccounts(ctx context.Context) (int, error)

	// Sessions (bearer tokens for the admin dashboard and CLI).
	CreateSession(ctx context.Context, s *Session) error
	GetSessionByID(ctx context.Context, id string) (*Session, error)
	DeleteSession(ctx context.Context, id string) error

	// Projects.
	GetProjectByID(ctx context.Context, id string) (*Project, error)
	ListProjects(ctx context.Context) ([]*Project, error)
	CreateProject(ctx context.Context, p *Project) error
	// UpdateProject writes the supplied project fields and bumps
	// config_version + updated_at. Returns the new config_version.
	UpdateProject(ctx context.Context, p *Project) (int64, error)
	DeleteProject(ctx context.Context, projectID string) error
	// RegenerateProjectSecret swaps in a new secret_key, bumps
	// config_version, and returns the new (secretKey, configVersion).
	RegenerateProjectSecret(ctx context.Context, projectID, newSecretKey string) (int64, error)

	// Databases.
	GetDatabase(ctx context.Context, projectID, id string) (*Database, error)
	ListDatabasesByProject(ctx context.Context, projectID string) ([]*Database, error)
	GetActiveDatabasesByProject(ctx context.Context, projectID string) ([]ActiveDatabaseAssignment, error)
	// CreateDatabase inserts an unassigned database row. Server assignment
	// happens on the first client connect; until then status='inactive'
	// and server_id IS NULL.
	CreateDatabase(ctx context.Context, projectID, id string, ephemeral bool) error
	EvictDatabases(ctx context.Context, evictions []EvictionRequest) error

	// Servers.
	GetServerByID(ctx context.Context, id string) (*Server, error)
	GetHealthyServers(ctx context.Context, heartbeatTimeout int64) ([]*Server, error)
	GetBestServer(ctx context.Context, heartbeatTimeout int64) (*Server, error)
	CreateServer(ctx context.Context, id, hostname, ipAddress, privateIP string, udpPort, capacity int) (*Server, error)
	UpdateServerHeartbeat(ctx context.Context, id string, dbCount, connCount int) error
	IncrementServerDatabaseCount(ctx context.Context, id string) error
	DecrementServerDatabaseCount(ctx context.Context, id string) error
	SetServerStatus(ctx context.Context, id, status string) error
	GetUnhealthyServers(ctx context.Context, deathTimeout int64) ([]*Server, error)
	ClearServerAssignments(ctx context.Context, serverID string) ([]DatabaseKey, error)
	ClearServerAssignmentsOnStartup(ctx context.Context, serverID string) ([]DatabaseKey, error)
	RegisterServer(ctx context.Context, serverID, privateIP string, proxyPort, nrCores int) error
	GetAllServersForDiscovery(ctx context.Context) ([]*Server, error)
	UpdateServerHeartbeatFromProxy(ctx context.Context, serverID string, load, clients, memMB int) error

	// Routing — the hot path for proxy clients.
	EnsureRoutingData(ctx context.Context, projectID, databaseID string, heartbeatTimeout int64) (*Server, *Project, error)
	AssignDatabase(ctx context.Context, projectID, databaseID string, ephemeral bool, heartbeatTimeout int64) (string, error)

	// Metrics. Ingestion is fed by the in-process aggregator (which reads
	// per-database metrics from /internal/metrics and flushes every few
	// minutes) and writes one database_metrics row per (project, database).
	// GetProjectMetricsRange rolls those rows up to project level for the
	// dashboard.
	InsertDatabaseMetricsBatch(ctx context.Context, metrics []*DatabaseMetricsRow) error
	InsertDatabaseEvent(ctx context.Context, e *DatabaseEvent) error
	GetProjectMetricsRange(ctx context.Context, projectID string, start, end time.Time) ([]*ProjectMetricsRow, error)
	ListDatabaseEvents(ctx context.Context, projectID string, limit, offset int) (events []*DatabaseEvent, total int, err error)

	// Notifications. Postgres only; returns ErrUnsupported on SQLite/Local.
	// Admin endpoints invoke the notify handler directly in-process, so
	// this interface is only used by callers that need to subscribe to
	// changes coming from an external writer.
	Listen(ctx context.Context, channels []string, handler func(Notification)) error
}
