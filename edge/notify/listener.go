// Package notify subscribes to Postgres NOTIFY channels used for
// admin-initiated state changes that bypass the normal client traffic path.
//
// Two channels are listened to:
//
//	project_config_changed   — project rules/secrets/ephemeral/etc. changed
//	database_evicted         — a database was deleted or is being unloaded
//
// Admin writes emit these inside the same transaction as the corresponding
// UPDATE/DELETE, so proxies only see the notification after the row change
// has committed.
//
// Every proxy in the mesh listens. Duplicate work is expected and tolerated:
//
//   - CONFIG_PUSH is deduped on the backend by comparing config_version against
//     the cached version.
//   - EVICT_DATABASE is idempotent (evicting an already-evicted database is a
//     no-op on the backend).
//
// Payloads are JSON. Fields are validated before dispatch; malformed messages
// are logged and skipped without killing the listener.
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

const (
	ChannelProjectConfigChanged = "project_config_changed"
	ChannelDatabaseEvicted      = "database_evicted"
)

// Handler dispatches decoded notifications. Implementations should return
// quickly (spawn goroutines for slow work) since the listener delivers
// notifications sequentially.
type Handler interface {
	// OnProjectConfigChanged is invoked when an admin write changes a
	// project's config. The handler is expected to invalidate any local
	// caches for this project and fan a CONFIG_PUSH out to every backend
	// core that has the project loaded, tagged with the given configVersion.
	OnProjectConfigChanged(projectID string, configVersion int64)

	// OnDatabaseEvicted is invoked when an admin write evicts a database.
	// If serverID is non-empty, it was looked up before the DELETE and the
	// handler can route directly. If empty, the handler should broadcast
	// EVICT_DATABASE to every connected backend — EVICT is idempotent, so
	// hitting backends that never had the database is a no-op.
	//
	// If purge is true, the backend should also remove any on-disk data for
	// persistent databases. Ephemeral databases have no on-disk state.
	OnDatabaseEvicted(projectID, databaseID, serverID string, purge bool)
}

// Listener subscribes to Postgres NOTIFY channels and dispatches to a Handler.
type Listener struct {
	db      db.Store
	handler Handler

	ctx    context.Context
	cancel context.CancelFunc
}

// NewListener creates a new listener. Call Start to begin receiving
// notifications; call Stop to cancel and clean up.
func NewListener(database db.Store, handler Handler) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{
		db:      database,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start runs the listener loop in a new goroutine. The loop reconnects with
// backoff if the LISTEN connection drops.
func (l *Listener) Start() {
	go l.run()
}

// Stop cancels the listener. Safe to call more than once.
func (l *Listener) Stop() {
	l.cancel()
}

func (l *Listener) run() {
	channels := []string{ChannelProjectConfigChanged, ChannelDatabaseEvicted}
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if l.ctx.Err() != nil {
			return
		}

		err := l.db.Listen(l.ctx, channels, l.dispatch)
		if l.ctx.Err() != nil {
			return
		}

		logger.Warn("Postgres notify listener disconnected, reconnecting", "error", err, "backoff", backoff)

		select {
		case <-l.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (l *Listener) dispatch(n db.Notification) {
	switch n.Channel {
	case ChannelProjectConfigChanged:
		var p projectConfigChangedPayload
		if err := json.Unmarshal([]byte(n.Payload), &p); err != nil {
			logger.Warn("Invalid project_config_changed payload", "error", err, "payload", n.Payload)
			return
		}
		if err := p.validate(); err != nil {
			logger.Warn("Invalid project_config_changed payload", "error", err, "payload", n.Payload)
			return
		}
		logger.Info("project_config_changed", "project_id", p.ProjectID, "config_version", p.ConfigVersion)
		l.handler.OnProjectConfigChanged(p.ProjectID, p.ConfigVersion)

	case ChannelDatabaseEvicted:
		var p databaseEvictedPayload
		if err := json.Unmarshal([]byte(n.Payload), &p); err != nil {
			logger.Warn("Invalid database_evicted payload", "error", err, "payload", n.Payload)
			return
		}
		if err := p.validate(); err != nil {
			logger.Warn("Invalid database_evicted payload", "error", err, "payload", n.Payload)
			return
		}
		logger.Info("database_evicted", "project_id", p.ProjectID, "database_id", p.DatabaseID, "server_id", p.ServerID, "purge", p.Purge)
		l.handler.OnDatabaseEvicted(p.ProjectID, p.DatabaseID, p.ServerID, p.Purge)

	default:
		logger.Warn("Unknown notify channel", "channel", n.Channel)
	}
}

type projectConfigChangedPayload struct {
	ProjectID     string `json:"project_id"`
	ConfigVersion int64  `json:"config_version"`
}

func (p projectConfigChangedPayload) validate() error {
	if p.ProjectID == "" {
		return fmt.Errorf("project_id is required")
	}
	if p.ConfigVersion <= 0 {
		return fmt.Errorf("config_version must be > 0")
	}
	return nil
}

type databaseEvictedPayload struct {
	ProjectID  string `json:"project_id"`
	DatabaseID string `json:"database_id"`
	ServerID   string `json:"server_id"`
	Purge      bool   `json:"purge"`
}

func (p databaseEvictedPayload) validate() error {
	if p.ProjectID == "" {
		return fmt.Errorf("project_id is required")
	}
	if p.DatabaseID == "" {
		return fmt.Errorf("database_id is required")
	}
	// server_id is optional — absent means "broadcast to all backends"
	return nil
}
