package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/db"
)

// TestInternalEndpointsRequireServerSecret covers security audit finding #5:
// /internal/* (server registration, metrics ingest) must reject callers that
// don't present Authorization: Bearer <SERVER_SECRET>. Otherwise anyone who can
// reach the internal listener could register a rogue backend and hijack routing.
func TestInternalEndpointsRequireServerSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lark.db")
	store, err := db.NewSqlite(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer store.Close()

	const secret = "s3cr3t-shared"
	s := New(&config.Config{ServerSecret: secret}, store)
	s.RegisterMetricsRoutes() // /internal/metrics is registered separately from New()

	body := `{"server_id":"db-1","address":"10.0.0.5:2727","nr_cores":4}`
	post := func(path, auth string) int {
		r := httptest.NewRequest("POST", path, bytes.NewReader([]byte(body)))
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w.Code
	}

	// /internal/register
	if code := post("/internal/register", ""); code != http.StatusUnauthorized {
		t.Errorf("register, no auth: got %d, want 401", code)
	}
	if code := post("/internal/register", "Bearer wrong-secret"); code != http.StatusUnauthorized {
		t.Errorf("register, wrong secret: got %d, want 401", code)
	}
	if code := post("/internal/register", secret); code != http.StatusUnauthorized {
		t.Errorf("register, missing Bearer prefix: got %d, want 401", code)
	}
	if code := post("/internal/register", "Bearer "+secret); code == http.StatusUnauthorized {
		t.Errorf("register, correct secret: got 401, want it to proceed")
	}

	// /internal/metrics is gated by the same middleware.
	if code := post("/internal/metrics", ""); code != http.StatusUnauthorized {
		t.Errorf("metrics, no auth: got %d, want 401", code)
	}
}
