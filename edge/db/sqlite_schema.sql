-- lark-edge schema (SQLite).
--
-- Applied automatically on first open via NewSqlite. Every CREATE uses
-- IF NOT EXISTS so reapplying is a no-op.

CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',            -- 'admin' or 'user'
    must_change_password INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_accounts_email ON accounts(email);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind TEXT NOT NULL DEFAULT 'dashboard',        -- 'dashboard' or 'cli'
    name TEXT,
    last_used_at INTEGER,
    created_ip TEXT,
    created_user_agent TEXT,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_account ON sessions(account_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_sessions_public_id ON sessions(public_id);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    secret_key TEXT NOT NULL,
    admin_secret_key TEXT NOT NULL,
    rules_json TEXT NOT NULL DEFAULT '{"rules":{".read":true,".write":true}}',
    ephemeral INTEGER NOT NULL DEFAULT 1,
    auth_required INTEGER NOT NULL DEFAULT 0,
    auto_create INTEGER NOT NULL DEFAULT 1,
    firebase_compat_enabled INTEGER NOT NULL DEFAULT 1,
    firebase_project_id TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    config_version INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_projects_updated_at ON projects(updated_at);

CREATE TABLE IF NOT EXISTS servers (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    private_ip TEXT NOT NULL DEFAULT '',
    udp_port INTEGER NOT NULL,
    proxy_port INTEGER NOT NULL DEFAULT 0,
    nr_cores INTEGER NOT NULL DEFAULT 1,
    last_heartbeat INTEGER NOT NULL,
    database_count INTEGER NOT NULL DEFAULT 0,
    connection_count INTEGER NOT NULL DEFAULT 0,
    capacity INTEGER NOT NULL DEFAULT 1000,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE INDEX IF NOT EXISTS idx_servers_status ON servers(status);
CREATE INDEX IF NOT EXISTS idx_servers_heartbeat ON servers(last_heartbeat);

CREATE TABLE IF NOT EXISTS databases (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    server_id TEXT REFERENCES servers(id) ON DELETE SET NULL,
    ephemeral INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'inactive',
    last_activity INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, id)
);

CREATE INDEX IF NOT EXISTS idx_databases_server ON databases(server_id);
CREATE INDEX IF NOT EXISTS idx_databases_status ON databases(status);
CREATE INDEX IF NOT EXISTS idx_databases_project ON databases(project_id);

-- Observability tables. Populated by the in-process metrics aggregator, which
-- writes one row per (project, database) per flush window. The dashboard rolls
-- these up to project level on read. Missing rows are treated as "no data."
CREATE TABLE IF NOT EXISTS database_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    database_id TEXT NOT NULL,
    ccu INTEGER NOT NULL,
    peak_ccu INTEGER NOT NULL,
    bytes_in INTEGER NOT NULL,
    bytes_out INTEGER NOT NULL,
    writes INTEGER NOT NULL,
    reads INTEGER NOT NULL,
    events_sent INTEGER NOT NULL,
    permission_denials INTEGER NOT NULL,
    connection_rejections INTEGER NOT NULL,
    data_size_bytes INTEGER NOT NULL,   -- current on-disk size (gauge)
    p50_latency_us INTEGER,
    p99_latency_us INTEGER
);

CREATE INDEX IF NOT EXISTS idx_database_metrics_lookup ON database_metrics (project_id, ts DESC);

CREATE TABLE IF NOT EXISTS database_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    database_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    details TEXT
);

CREATE INDEX IF NOT EXISTS idx_db_events_project_ts ON database_events (project_id, ts DESC);
