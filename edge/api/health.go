package api

import (
	"net/http"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Check for live backend connections (local state, no DB query)
	if s.pool == nil || len(s.pool.GetHealthyBackendIDs()) == 0 {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "error",
			"error":  "no backend servers available",
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
