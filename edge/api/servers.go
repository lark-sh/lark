package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/lark-sh/lark/edge/db"
)

// RegisterServerRequest is the request body for server registration
type RegisterServerRequest struct {
	ServerID string `json:"server_id"`
	Address  string `json:"address"` // private_ip:port
	NrCores  int    `json:"nr_cores"`
}

// handleRegisterServer handles POST /internal/register
// Called by lark-server on startup to register with the coordinator.
// Only accessible via internal HTTP server (port 8080).
func (s *Server) handleRegisterServer(w http.ResponseWriter, r *http.Request) {
	var req RegisterServerRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Validate required fields
	if req.ServerID == "" {
		s.writeError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	if req.Address == "" {
		s.writeError(w, http.StatusBadRequest, "address is required")
		return
	}
	if req.NrCores <= 0 {
		s.writeError(w, http.StatusBadRequest, "nr_cores must be positive")
		return
	}

	// Parse address into private_ip and port
	parts := strings.Split(req.Address, ":")
	if len(parts) != 2 {
		s.writeError(w, http.StatusBadRequest, "address must be in format ip:port")
		return
	}
	privateIP := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil || port <= 0 || port > 65535 {
		s.writeError(w, http.StatusBadRequest, "invalid port in address")
		return
	}

	// Register in database
	if err := s.db.RegisterServer(r.Context(), req.ServerID, privateIP, port, req.NrCores); err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to register server")
		return
	}

	// Kick off a discovery pass so this edge connects to the new server
	// immediately and marks it healthy, rather than waiting for the next
	// periodic discovery tick. Runs in the background; we don't block the
	// registration response on the connection handshake.
	if s.pool != nil {
		s.pool.TriggerDiscovery()
	}

	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// DeregisterServerRequest is the request body for server deregistration
type DeregisterServerRequest struct {
	ServerID string `json:"server_id"`
}

// handleDeregisterServer handles POST /internal/deregister
// Called by lark-server when it starts a graceful shutdown. The server is
// marked stopped, so routing stops sending clients to it and its databases are
// reassigned on their next connect. It's marked active again only when it
// registers on its next start.
func (s *Server) handleDeregisterServer(w http.ResponseWriter, r *http.Request) {
	var req DeregisterServerRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ServerID == "" {
		s.writeError(w, http.StatusBadRequest, "server_id is required")
		return
	}

	if err := s.db.SetServerStatus(r.Context(), req.ServerID, db.ServerStatusStopped); err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to deregister server")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
