package db

import (
	"strconv"
	"time"
)

// Project represents a project/app.
type Project struct {
	ID                            string `json:"id"`
	Name                          string `json:"name"`
	SecretKey                     string `json:"secret_key"`
	AdminSecretKey                string `json:"admin_secret_key"`
	RulesJSON                     string `json:"rules_json"`
	Ephemeral                     bool   `json:"ephemeral"`
	AutoCreate                    bool   `json:"auto_create"`
	FirebaseCompatEnabled         bool   `json:"firebase_compat_enabled"`
	FirebaseProjectID             string `json:"firebase_project_id"`
	UseFirstPathSegmentAsDatabase bool   `json:"use_first_path_segment_as_database"`
	ConfigVersion                 int64  `json:"config_version"`
	CreatedAt                     int64  `json:"created_at"`
	UpdatedAt                     int64  `json:"updated_at"`
}

// Server represents a backend database server.
type Server struct {
	ID              string `json:"id"`
	Hostname        string `json:"hostname"`
	IPAddress       string `json:"ip_address"`
	PrivateIP       string `json:"private_ip"`
	UDPPort         int    `json:"udp_port"`
	ProxyPort       int    `json:"proxy_port"` // Port for proxy TCP connections
	NrCores         int    `json:"nr_cores"`   // Number of cores for sharding
	LastHeartbeat   int64  `json:"last_heartbeat"`
	DatabaseCount   int    `json:"database_count"`
	ConnectionCount int    `json:"connection_count"`
	Capacity        int    `json:"capacity"`
	Status          string `json:"status"` // active, draining, offline, pending
}

// ProxyAddress returns the address for proxy connections (private_ip:proxy_port).
func (s *Server) ProxyAddress() string {
	return s.PrivateIP + ":" + strconv.Itoa(s.ProxyPort)
}

// Database represents a database instance.
type Database struct {
	ProjectID    string `json:"project_id"`
	ID           string `json:"id"`
	ServerID     string `json:"server_id"`
	Ephemeral    bool   `json:"ephemeral"`
	Status       string `json:"status"` // inactive, active, evicting
	LastActivity int64  `json:"last_activity"`
	CreatedAt    int64  `json:"created_at"`
}

// ConnectData holds the combined data needed for routing.
type ConnectData struct {
	Project  *Project
	Database *Database
	Server   *Server
}

// DatabaseKey is a (project_id, database_id) pair.
type DatabaseKey struct {
	ProjectID string
	ID        string
}

// ActiveDatabaseAssignment is a (database_id, server_id) pair for a database
// that is currently assigned to a backend. Used by the project_config_changed
// NOTIFY handler to fan CONFIG_PUSH out to every core the project is running on.
type ActiveDatabaseAssignment struct {
	DatabaseID string
	ServerID   string
}

// EvictionRequest names a database to evict.
type EvictionRequest struct {
	ProjectID  string
	DatabaseID string
}

// Notification is a single Postgres NOTIFY event delivered to a Listen handler.
type Notification struct {
	Channel string
	Payload string
}

// DatabaseMetricsRow represents a row in the database_metrics table — one
// usage sample per (project, database) per flush window. This is the unit the
// metrics aggregator writes; the dashboard rolls these up to project level on
// read (see GetProjectMetricsRange).
type DatabaseMetricsRow struct {
	ID                   int64     `json:"id"`
	Timestamp            time.Time `json:"ts"`
	ProjectID            string    `json:"project_id"`
	DatabaseID           string    `json:"database_id"`
	CCU                  int       `json:"ccu"`
	PeakCCU              int       `json:"peak_ccu"`
	BytesIn              int64     `json:"bytes_in"`
	BytesOut             int64     `json:"bytes_out"`
	Writes               int64     `json:"writes"`
	Reads                int64     `json:"reads"`
	EventsSent           int64     `json:"events_sent"`
	PermissionDenials    int       `json:"permission_denials"`
	ConnectionRejections int       `json:"connection_rejections"`
	DataSizeBytes        int64     `json:"data_size_bytes"` // current on-disk size (gauge)
	P50LatencyUs         int       `json:"p50_latency_us"`
	P99LatencyUs         int       `json:"p99_latency_us"`
}

// ProjectMetricsRow is a per-project rollup of database_metrics, served to the
// dashboard. There is no project_metrics table — these rows are produced by
// aggregating database_metrics on read (GROUP BY ts within a project).
type ProjectMetricsRow struct {
	Timestamp            time.Time `json:"ts"`
	ProjectID            string    `json:"project_id"`
	CCU                  int       `json:"ccu"`
	PeakCCU              int       `json:"peak_ccu"`
	BytesIn              int64     `json:"bytes_in"`
	BytesOut             int64     `json:"bytes_out"`
	Writes               int64     `json:"writes"`
	Reads                int64     `json:"reads"`
	EventsSent           int64     `json:"events_sent"`
	PermissionDenials    int       `json:"permission_denials"`
	ConnectionRejections int       `json:"connection_rejections"`
	P50LatencyUs         int       `json:"p50_latency_us"`
	P99LatencyUs         int       `json:"p99_latency_us"`
}

// Account is a dashboard user. Passwords are bcrypt-hashed.
type Account struct {
	ID                 string `json:"id"`
	Email              string `json:"email"`
	PasswordHash       string `json:"-"` // never marshalled
	Role               string `json:"role"`
	MustChangePassword bool   `json:"must_change_password"`
	CreatedAt          int64  `json:"created_at"`
}

// Session is a server-side record of a logged-in dashboard or CLI session.
// `ID` is the bearer token (cookie value); `PublicID` is a separate opaque
// identifier used in API responses so the bearer token never has to be
// exposed over JSON.
type Session struct {
	ID               string `json:"-"`
	PublicID         string `json:"id"`
	AccountID        string `json:"account_id"`
	Kind             string `json:"kind"` // 'dashboard' or 'cli'
	Name             string `json:"name,omitempty"`
	LastUsedAt       int64  `json:"last_used_at,omitempty"`
	CreatedIP        string `json:"created_ip,omitempty"`
	CreatedUserAgent string `json:"created_user_agent,omitempty"`
	ExpiresAt        int64  `json:"expires_at"`
	CreatedAt        int64  `json:"created_at"`
}

// DatabaseEvent represents a row in the database_events table.
type DatabaseEvent struct {
	ID         int64     `json:"id"`
	Timestamp  time.Time `json:"ts"`
	ProjectID  string    `json:"project_id"`
	DatabaseID string    `json:"database_id"`
	EventType  string    `json:"event_type"`
	Message    string    `json:"message"`
	Details    string    `json:"details,omitempty"` // JSON string
}
