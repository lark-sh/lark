package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/db"
)

// newSPATestServer spins up an SPA-mounted server for the static-handler
// tests. DisableTLS=true so the Secure cookie flag isn't set (httptest
// doesn't speak TLS, and Secure cookies wouldn't survive the round-trip).
func newSPATestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lark.db")
	store, err := db.NewSqlite(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	cfg := &config.Config{
		AdminAPIEnabled: true,
		DisableTLS:      true,
	}
	s := New(cfg, store)
	return s, func() { store.Close() }
}

func getRaw(s *Server, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func TestSPA_ServesPlaceholderAtRoot(t *testing.T) {
	s, cleanup := newSPATestServer(t)
	defer cleanup()

	rr := getRaw(s, "/admin/")
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "<html") {
		t.Errorf("expected HTML, got: %s", body[:min(80, len(body))])
	}
}

func TestSPA_UnknownPathFallsBackToIndex(t *testing.T) {
	s, cleanup := newSPATestServer(t)
	defer cleanup()

	// A client-side route like /admin/projects/abc — no such file in dist.
	// We expect index.html so React Router can resolve it on the client.
	rr := getRaw(s, "/admin/projects/abc")
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "<html") {
		t.Errorf("expected SPA HTML fallback, got: %s", body[:min(80, len(body))])
	}
}

func TestSPA_BareAdminRedirects(t *testing.T) {
	s, cleanup := newSPATestServer(t)
	defer cleanup()

	rr := getRaw(s, "/admin")
	if rr.Code != http.StatusMovedPermanently {
		t.Errorf("status: got %d, want 301", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/admin/" {
		t.Errorf("Location: got %q, want /admin/", loc)
	}
}

func TestSPA_APIRoutesTakePrecedence(t *testing.T) {
	// Sanity check that the catch-all SPA handler doesn't shadow the
	// JSON API routes registered for the same /admin/ prefix.
	s, cleanup := newSPATestServer(t)
	defer cleanup()

	rr := getRaw(s, "/admin/api/me")
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("GET /admin/api/me (unauthenticated): got %d, want 401", rr.Code)
	}
	// Response should be JSON-ish, not HTML.
	if strings.Contains(rr.Body.String(), "<html") {
		t.Errorf("API route returned HTML; SPA handler is shadowing it")
	}
}

func TestSPA_DisabledWhenAdminFlagOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lark.db")
	store, _ := db.NewSqlite(context.Background(), "sqlite://"+path)
	defer store.Close()
	s := New(&config.Config{AdminAPIEnabled: false}, store)

	rr := getRaw(s, "/admin/")
	if rr.Code != http.StatusNotFound {
		t.Errorf("with admin disabled: got %d, want 404", rr.Code)
	}
}
