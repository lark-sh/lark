// Package cron provides background maintenance jobs for the Lark proxy.
//
// # Overview
//
// This package runs periodic cleanup tasks to maintain system health. The primary
// job handles server failure detection and database reassignment.
//
// # Server Health Monitoring
//
// Backend servers send heartbeats via the wire protocol. The cleanup job detects
// servers that have stopped sending heartbeats and performs cleanup:
//
//  1. After DEATH_TIMEOUT seconds with no heartbeat:
//     - Server status is set to "offline"
//     - Ephemeral databases on that server are deleted
//     - Persistent databases have their server_id cleared (will be reassigned)
//
// This is a safety net. The primary failure detection happens on-demand when
// clients connect (via assign_database) or when the backend connection dies.
//
// # Job Schedule
//
// Cleanup runs every 1 minute. The first run happens immediately on startup.
// Each run has a 30-second timeout to prevent blocking.
//
// # Idempotency
//
// All cleanup operations are idempotent. Multiple proxies can run cleanup
// concurrently without conflict. This is important because all proxies in
// the mesh run the same cleanup job.
//
// # Future Jobs
//
// This package can be extended with additional background tasks:
//   - Session expiration
//   - Usage metrics aggregation
//   - Stale database cleanup
package cron

import (
	"context"
	"time"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

// Cleanup runs periodic cleanup tasks
type Cleanup struct {
	db           db.Store
	deathTimeout int64 // seconds

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCleanup creates a new cleanup job
func NewCleanup(database db.Store, deathTimeout int) *Cleanup {
	ctx, cancel := context.WithCancel(context.Background())
	return &Cleanup{
		db:           database,
		deathTimeout: int64(deathTimeout),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Run starts the cleanup job (runs every minute)
func (c *Cleanup) Run() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Run once immediately
	c.runCleanup()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.runCleanup()
		}
	}
}

// runCleanup performs the actual cleanup
func (c *Cleanup) runCleanup() {
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	// Find unhealthy servers
	unhealthyServers, err := c.db.GetUnhealthyServers(ctx, c.deathTimeout)
	if err != nil {
		logger.Error("Error getting unhealthy servers", "error", err)
		return
	}

	if len(unhealthyServers) == 0 {
		return
	}

	for _, server := range unhealthyServers {
		logger.Warn("Server is unhealthy", "server_id", server.ID, "last_heartbeat_ms_ago", db.NowMS()-server.LastHeartbeat)

		// Get databases assigned to this server
		affectedDBs, err := c.db.ClearServerAssignments(ctx, server.ID)
		if err != nil {
			logger.Error("Error clearing assignments", "server_id", server.ID, "error", err)
			continue
		}

		// Mark server as offline
		err = c.db.SetServerStatus(ctx, server.ID, "offline")
		if err != nil {
			logger.Error("Error setting server offline", "server_id", server.ID, "error", err)
		}

		logger.Info("Cleared database assignments", "count", len(affectedDBs), "server_id", server.ID)
	}
}

// Stop stops the cleanup job
func (c *Cleanup) Stop() {
	c.cancel()
}
