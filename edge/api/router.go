// Package api provides the HTTP API server for lark-edge.
//
// # Endpoints
//
// Infrastructure (internal network only):
//   - POST /internal/register  - Backend server registration on startup
//   - POST /internal/metrics   - Metrics ingestion from the upstream shipper
//
// Health:
//   - GET /health - Health check (checks live backend connections)
package api

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
	"github.com/lark-sh/lark/edge/metrics"
	"github.com/lark-sh/lark/edge/notify"
)

// Server is the HTTP API server
type Server struct {
	config            *config.Config
	db                db.Store
	pool              BackendPool
	mux               *http.ServeMux
	metricsAggregator *metrics.MetricsAggregator

	// notifyHandler dispatches admin-initiated state changes (config
	// updates, evictions) to the proxy caches and backends. Admin write
	// handlers call into this directly after committing their DB write.
	notifyHandler notify.Handler

	// loginThrottle slows repeated failed admin logins per account (audit L-2).
	loginThrottle *loginThrottle
}

// BackendPool is the interface for checking backend health
type BackendPool interface {
	GetHealthyBackendIDs() []string
}

// New creates a new API server
func New(cfg *config.Config, database db.Store) *Server {
	s := &Server{
		config:        cfg,
		db:            database,
		mux:           http.NewServeMux(),
		loginThrottle: newLoginThrottle(),
	}

	s.registerRoutes()
	return s
}

// SetPool sets the backend pool for health checks
func (s *Server) SetPool(pool BackendPool) {
	s.pool = pool
}

// SetNotifyHandler wires the dispatcher that admin write paths fan out
// through after committing.
func (s *Server) SetNotifyHandler(h notify.Handler) {
	s.notifyHandler = h
}

func (s *Server) registerRoutes() {
	// Health check
	s.mux.HandleFunc("GET /health", s.handleHealth)

	// Server-to-server endpoints. Reachable only on the internal listener (the
	// public listener 404s /internal/*), AND authenticated with SERVER_SECRET so
	// network isolation isn't the only line of defense — an unauthenticated caller
	// that reaches the internal port still can't register a rogue backend.
	s.mux.HandleFunc("POST /internal/register", s.requireServerSecret(s.handleRegisterServer))

	if s.config.AdminAPIEnabled {
		s.registerAdminRoutes()
		s.mountAdminSPA()
	}
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Handler returns the HTTP handler
func (s *Server) Handler() http.Handler {
	return s
}

// requireServerSecret wraps an internal server-to-server handler so it only runs
// for callers presenting `Authorization: Bearer <SERVER_SECRET>`. The compare is
// constant-time. This is what stops an attacker who can merely reach the internal
// listener from registering a rogue backend or poisoning metrics.
func (s *Server) requireServerSecret(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		provided, ok := strings.CutPrefix(r.Header.Get("Authorization"), prefix)
		if !ok || subtle.ConstantTimeCompare([]byte(provided), []byte(s.config.ServerSecret)) != 1 {
			s.writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

// Helper functions

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logger.Error("Error encoding JSON response", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

// maxRequestBodySize is the maximum allowed request body size (10MB).
const maxRequestBodySize = 10 * 1024 * 1024

func (s *Server) readJSON(w http.ResponseWriter, r *http.Request, v interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	return json.NewDecoder(r.Body).Decode(v)
}
