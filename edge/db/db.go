// Package db provides the data-store layer for lark-edge.
//
// # The Store interface
//
// Every caller outside this package depends on the [Store] interface, never
// a concrete type. Three implementations live here:
//
//   - [PostgresDB] — Postgres backend.
//   - [SqliteDB]   — single-file SQLite backend.
//   - [LocalDB]    — in-memory mock for tests and LOCAL_MODE.
//
// Use [New] to open whichever one DATABASE_URL points at; the returned
// Store hides the dialect.
//
// Driver selection is by URL scheme:
//
//   - "postgres://" / "postgresql://"  →  Postgres
//   - "sqlite://" / "file:"            →  SQLite
//
// Postgres-only features (LISTEN/NOTIFY) return [ErrUnsupported] on SQLite.
// Admin endpoints invoke the notify handler in-process, so there's no need
// to LISTEN to your own writes.
//
// # Key tables
//
//   - projects     — per-project configuration (rules, auth settings, etc.)
//   - databases    — database instances, with current server assignment
//   - servers      — backend server registry with health metrics
//   - accounts     — admin user accounts
//   - sessions     — login sessions for the admin UI
//
// # Routing
//
// The most critical entry point is database-to-server assignment (see
// routing.go and the AssignDatabase method on each impl):
//
//  1. GetConnectData fetches project config and current server assignment.
//  2. If the database is unassigned, AssignDatabase atomically picks a
//     healthy server via a deterministic hash (same key → same server).
//  3. Unhealthy servers are detected via heartbeat timeout and bypassed.
//
// # Thread safety
//
// All Store methods are safe for concurrent use.
package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrNotFound is returned when a lookup finds no matching row.
var ErrNotFound = errors.New("not found")

// ErrUnsupported is returned by methods that aren't supported on the current
// driver — e.g. Listen on SQLite.
var ErrUnsupported = errors.New("not supported on this driver")

// Driver identifies which backend a Store is using.
type Driver int

const (
	DriverPostgres Driver = iota
	DriverSQLite
	DriverLocal
)

func (d Driver) String() string {
	switch d {
	case DriverPostgres:
		return "postgres"
	case DriverSQLite:
		return "sqlite"
	case DriverLocal:
		return "local"
	default:
		return "unknown"
	}
}

// New opens the store described by databaseURL. The URL scheme selects the
// driver. For SQLite, the file is created if it doesn't exist and the
// embedded schema is applied on first open.
func New(ctx context.Context, databaseURL string) (Store, error) {
	switch {
	case strings.HasPrefix(databaseURL, "postgres://"),
		strings.HasPrefix(databaseURL, "postgresql://"):
		return NewPostgres(ctx, databaseURL)
	case strings.HasPrefix(databaseURL, "sqlite://"),
		strings.HasPrefix(databaseURL, "file:"):
		return NewSqlite(ctx, databaseURL)
	}
	return nil, fmt.Errorf("unrecognized DATABASE_URL scheme: %q (expected postgres://, postgresql://, sqlite://, or file:)", databaseURL)
}

// NowMS returns the current Unix timestamp in milliseconds.
func NowMS() int64 {
	return time.Now().UnixMilli()
}
