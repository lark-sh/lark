package api

import (
	"net/http"
)

// adminStatsResponse is the high-level snapshot the dashboard's home
// screen renders. Fields are deliberately coarse; detailed per-project
// metrics live in /admin/projects/{id}/metrics (to be added when the
// metrics ingestion path lands).
type adminStatsResponse struct {
	Accounts       int `json:"accounts"`
	Projects       int `json:"projects"`
	Databases      int `json:"databases"`
	HealthyServers int `json:"healthy_servers"`
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	var out adminStatsResponse

	if n, err := s.db.CountAccounts(r.Context()); err == nil {
		out.Accounts = n
	}
	if projects, err := s.db.ListProjects(r.Context()); err == nil {
		out.Projects = len(projects)
		for _, p := range projects {
			if dbs, err := s.db.ListDatabasesByProject(r.Context(), p.ID); err == nil {
				out.Databases += len(dbs)
			}
		}
	}
	// 30s heartbeat window matches the default HeartbeatTimeout.
	if servers, err := s.db.GetHealthyServers(r.Context(), 30); err == nil {
		out.HealthyServers = len(servers)
	}

	s.writeJSON(w, http.StatusOK, out)
}
