package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lark-sh/lark/edge/config"
)

// mockPool implements BackendPool for testing
type mockPool struct {
	backends []string
}

func (m *mockPool) GetHealthyBackendIDs() []string {
	return m.backends
}

func (m *mockPool) TriggerDiscovery() {}

func TestHealthEndpoint(t *testing.T) {
	cfg := &config.Config{
		HeartbeatTimeout: 30,
	}

	server := New(cfg, nil)
	server.SetPool(&mockPool{backends: []string{"server-1"}}) // Mock healthy backend

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Status: got %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if body["status"] != "ok" {
		t.Errorf("Status: got %q, want %q", body["status"], "ok")
	}
}

func TestHealthEndpointNoBackends(t *testing.T) {
	cfg := &config.Config{
		HeartbeatTimeout: 30,
	}

	server := New(cfg, nil)
	// No pool set - should return 503

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Status: got %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if body["status"] != "error" {
		t.Errorf("Status: got %q, want %q", body["status"], "error")
	}
}
