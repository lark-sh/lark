-- lark-edge schema (Postgres).
-- Run with: psql $DATABASE_URL -f db/postgres_schema.sql
-- WARNING: This drops all existing tables!

DROP TABLE IF EXISTS database_events CASCADE;
DROP TABLE IF EXISTS database_metrics CASCADE;
DROP TABLE IF EXISTS databases CASCADE;
DROP TABLE IF EXISTS servers CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

-- Dashboard accounts.
CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',            -- 'admin' or 'user'
    must_change_password BOOLEAN NOT NULL DEFAULT false,
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_accounts_email ON accounts(email);

-- Sessions. `id` is the bearer token (cookie value); `public_id` is a
-- separate opaque identifier used in API responses so we never expose the
-- bearer token over JSON.
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind TEXT NOT NULL DEFAULT 'dashboard',        -- 'dashboard' or 'cli'
    name TEXT,                                      -- display label (e.g. "Chrome on macOS", "CLI")
    last_used_at BIGINT,
    created_ip TEXT,
    created_user_agent TEXT,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_sessions_account ON sessions(account_id);
CREATE INDEX idx_sessions_expires ON sessions(expires_at);
CREATE INDEX idx_sessions_public_id ON sessions(public_id);

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    secret_key TEXT NOT NULL,
    admin_secret_key TEXT NOT NULL,
    rules_json TEXT NOT NULL DEFAULT '{"rules":{".read":true,".write":true}}',
    ephemeral BOOLEAN NOT NULL DEFAULT true,
    auth_required BOOLEAN NOT NULL DEFAULT false,
    auto_create BOOLEAN NOT NULL DEFAULT true,
    firebase_compat_enabled BOOLEAN NOT NULL DEFAULT true,
    firebase_project_id TEXT NOT NULL DEFAULT '',
    use_first_path_segment_as_database BOOLEAN NOT NULL DEFAULT false,
    config_version BIGINT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX idx_projects_updated_at ON projects(updated_at);

CREATE TABLE servers (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    private_ip TEXT NOT NULL DEFAULT '',
    udp_port INTEGER NOT NULL,
    proxy_port INTEGER NOT NULL DEFAULT 0,
    nr_cores INTEGER NOT NULL DEFAULT 1,
    last_heartbeat BIGINT NOT NULL,
    database_count INTEGER NOT NULL DEFAULT 0,
    connection_count INTEGER NOT NULL DEFAULT 0,
    capacity INTEGER NOT NULL DEFAULT 1000,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE INDEX idx_servers_status ON servers(status);
CREATE INDEX idx_servers_heartbeat ON servers(last_heartbeat);

CREATE TABLE databases (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    server_id TEXT REFERENCES servers(id) ON DELETE SET NULL,
    ephemeral BOOLEAN NOT NULL DEFAULT true,
    status TEXT NOT NULL DEFAULT 'inactive',
    last_activity BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (project_id, id)
);

CREATE INDEX idx_databases_server ON databases(server_id);
CREATE INDEX idx_databases_status ON databases(status);
CREATE INDEX idx_databases_project ON databases(project_id);

-- Helper: current Unix timestamp in milliseconds.
CREATE OR REPLACE FUNCTION now_ms() RETURNS BIGINT AS $$
BEGIN
    RETURN (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- assign_database atomically picks (or keeps) a healthy server for a
-- (project_id, database_id) and persists the assignment. Returns the
-- chosen server_id, or NULL if no healthy servers are available. Mirrors
-- the SqliteDB.AssignDatabase Go implementation.
CREATE OR REPLACE FUNCTION assign_database(
    p_project_id TEXT,
    p_database_id TEXT,
    p_ephemeral BOOLEAN,
    p_heartbeat_timeout_ms BIGINT
) RETURNS TEXT AS $$
DECLARE
    v_server_id TEXT;
    v_cutoff BIGINT;
BEGIN
    v_cutoff := now_ms() - p_heartbeat_timeout_ms;

    -- Check existing assignment first (fast path).
    SELECT server_id INTO v_server_id
    FROM databases
    WHERE project_id = p_project_id AND id = p_database_id;

    IF v_server_id IS NOT NULL THEN
        IF EXISTS (
            SELECT 1 FROM servers
            WHERE id = v_server_id AND status = 'active' AND last_heartbeat > v_cutoff
        ) THEN
            UPDATE databases
            SET status = 'active', last_activity = now_ms()
            WHERE project_id = p_project_id AND id = p_database_id AND status = 'inactive';
            RETURN v_server_id;
        END IF;
        -- Server is unhealthy — clear THIS database's assignment so we can reassign.
        UPDATE databases
        SET server_id = NULL
        WHERE project_id = p_project_id AND id = p_database_id;
        v_server_id := NULL;
    END IF;

    -- Pick a healthy server using a deterministic hash.
    SELECT id INTO v_server_id
    FROM (
        SELECT id, ROW_NUMBER() OVER (ORDER BY id) - 1 AS idx,
               COUNT(*) OVER () AS total
        FROM servers
        WHERE status = 'active' AND last_heartbeat > v_cutoff
    ) s
    WHERE idx = ABS(hashtext(p_project_id || '/' || p_database_id)) % total;

    IF v_server_id IS NULL THEN
        RETURN NULL;  -- No healthy servers.
    END IF;

    -- Upsert the assignment. ON CONFLICT preserves any concurrent winner.
    INSERT INTO databases (project_id, id, server_id, ephemeral, status, last_activity, created_at)
    VALUES (p_project_id, p_database_id, v_server_id, p_ephemeral, 'active', now_ms(), now_ms())
    ON CONFLICT (project_id, id) DO UPDATE
    SET server_id = COALESCE(databases.server_id, EXCLUDED.server_id),
        status = CASE WHEN databases.server_id IS NULL THEN 'active' ELSE databases.status END,
        last_activity = CASE WHEN databases.server_id IS NULL THEN now_ms() ELSE databases.last_activity END;

    -- Read back the (possibly raced) final assignment.
    SELECT server_id INTO v_server_id
    FROM databases
    WHERE project_id = p_project_id AND id = p_database_id;

    RETURN v_server_id;
END;
$$ LANGUAGE plpgsql;

-- Observability tables. Populated by the in-process metrics aggregator, which
-- writes one row per (project, database) per flush window. The dashboard rolls
-- these up to project level on read. Missing rows are treated as "no data."
CREATE TABLE database_metrics (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    project_id TEXT NOT NULL,
    database_id TEXT NOT NULL,
    ccu INT NOT NULL,
    peak_ccu INT NOT NULL,
    bytes_in BIGINT NOT NULL,
    bytes_out BIGINT NOT NULL,
    writes BIGINT NOT NULL,
    reads BIGINT NOT NULL,
    events_sent BIGINT NOT NULL,
    permission_denials INT NOT NULL,
    connection_rejections INT NOT NULL,
    data_size_bytes BIGINT NOT NULL,   -- current on-disk size (gauge)
    p50_latency_us INT,
    p99_latency_us INT
);

CREATE INDEX idx_database_metrics_lookup ON database_metrics (project_id, ts DESC);

CREATE TABLE database_events (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project_id TEXT NOT NULL,
    database_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSONB
);

CREATE INDEX idx_db_events_project_ts ON database_events (project_id, ts DESC);
