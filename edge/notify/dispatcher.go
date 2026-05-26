package notify

import (
	"context"
	"time"

	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

// ProjectConfigSource fetches the current project config for use in
// CONFIG_PUSH payloads. Wired up in main.go from the same adapter the
// backend pool uses, so the dispatcher and the backend pool see the same
// config view.
type ProjectConfigSource interface {
	GetProjectConfig(projectID string) (*backend.ProjectConfig, error)
}

// ProxyCache exposes the proxy server's project-config cache to the
// dispatcher so it can invalidate stale entries on project_config_changed.
type ProxyCache interface {
	InvalidateProjectCache(projectID string)
}

// Dispatcher fans admin-initiated state changes out to the proxy's caches
// and the backend wire protocol. It is both the concrete [Handler]
// implementation consumed by [Listener] and the entry point admin write
// paths call directly (skipping the LISTEN round-trip).
//
// Routing is derived entirely from the database + the deterministic core
// hash, so the dispatcher works correctly on a freshly restarted proxy
// that has no in-memory DATABASE_LOADED state yet. Multiple proxies
// receiving the same event all do the same work; backends dedupe
// CONFIG_PUSH by config_version and treat EVICT_DATABASE as idempotent.
type Dispatcher struct {
	db     db.Store
	pool   *backend.Pool
	config ProjectConfigSource
	cache  ProxyCache
}

// NewDispatcher wires the dependencies. None of them may be nil.
func NewDispatcher(database db.Store, pool *backend.Pool, config ProjectConfigSource, cache ProxyCache) *Dispatcher {
	return &Dispatcher{
		db:     database,
		pool:   pool,
		config: config,
		cache:  cache,
	}
}

// Compile-time check that Dispatcher satisfies the Handler interface.
var _ Handler = (*Dispatcher)(nil)

func (d *Dispatcher) OnProjectConfigChanged(projectID string, configVersion int64) {
	// Drop the proxy's project cache entry so future client requests see
	// the new config.
	d.cache.InvalidateProjectCache(projectID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := d.config.GetProjectConfig(projectID)
	if err != nil {
		logger.Warn("project_config_changed: failed to fetch config", "project_id", projectID, "error", err)
		return
	}
	if config.ConfigVersion < configVersion {
		// Caller claims a newer version than the row we just read. NOTIFY
		// only fires after COMMIT (and admin write paths call us after
		// their write commits), so this implies replication lag or an
		// out-of-order read; log and proceed with what we have.
		logger.Warn("project_config_changed: db version behind event", "project_id", projectID, "db_version", config.ConfigVersion, "event_version", configVersion)
	}

	assignments, err := d.db.GetActiveDatabasesByProject(ctx, projectID)
	if err != nil {
		logger.Warn("project_config_changed: failed to list active databases", "project_id", projectID, "error", err)
		return
	}

	// Fan out CONFIG_PUSH to every (server, core) pair that has a database
	// of this project loaded. Core is derived via the same hash the
	// backend uses, so no in-memory projectCores map is needed.
	pushed := 0
	for _, a := range assignments {
		b, err := d.pool.GetBackend(a.ServerID)
		if err != nil {
			// This proxy isn't connected to that backend; another proxy
			// will push.
			continue
		}
		coreID := backend.CoreForDatabase(projectID+"/"+a.DatabaseID, b.NrCores())
		if err := b.SendConfigPushToCore(coreID, projectID, config); err != nil {
			logger.Warn("project_config_changed: CONFIG_PUSH failed", "project_id", projectID, "database_id", a.DatabaseID, "server_id", a.ServerID, "core_id", coreID, "error", err)
			continue
		}
		pushed++
	}

	logger.Info("project_config_changed: pushed", "project_id", projectID, "config_version", config.ConfigVersion, "pushed", pushed, "assignments", len(assignments))
}

func (d *Dispatcher) OnDatabaseEvicted(projectID, databaseID, serverID string, purge bool) {
	if serverID != "" {
		// Targeted route: the caller looked up the server before the delete.
		b, err := d.pool.GetBackend(serverID)
		if err != nil {
			// Not connected to this backend from this proxy; another
			// proxy will handle it.
			return
		}
		if err := b.SendEvictDatabase(projectID, databaseID, purge); err != nil {
			logger.Warn("database_evicted: EVICT_DATABASE failed", "project_id", projectID, "database_id", databaseID, "server_id", serverID, "purge", purge, "error", err)
			return
		}
		logger.Info("database_evicted: evicted", "project_id", projectID, "database_id", databaseID, "server_id", serverID, "purge", purge)
		return
	}

	// Broadcast route: caller didn't know which backend owned the database
	// (common on delete — the databases row is gone by the time the event
	// fires). EVICT is idempotent so hitting backends that never had the
	// database is a no-op.
	sent := d.pool.EvictDatabaseOnAllBackends(projectID, databaseID, purge)
	logger.Info("database_evicted: broadcast", "project_id", projectID, "database_id", databaseID, "purge", purge, "backends", sent)
}
