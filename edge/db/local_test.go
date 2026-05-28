package db

import (
	"context"
	"testing"
)

// EnsureRoutingData must validate the database ID even in local mode, since it
// flows into the server's on-disk data-dir path (mirrors SQLite/Postgres).
func TestLocalDB_EnsureRoutingData_RejectsUnsafeDatabaseID(t *testing.T) {
	db := NewLocalDB("p1", "127.0.0.1:2727")
	ctx := context.Background()

	for _, bad := range []string{"../../etc", "..", "a/b", "UPPER", "trailing-", ""} {
		if _, _, err := db.EnsureRoutingData(ctx, "p1", bad, 30); err == nil {
			t.Errorf("expected rejection for databaseID %q", bad)
		}
	}

	// A DNS-safe id is accepted.
	if _, _, err := db.EnsureRoutingData(ctx, "p1", "valid-db", 30); err != nil {
		t.Errorf("valid databaseID rejected: %v", err)
	}
}
