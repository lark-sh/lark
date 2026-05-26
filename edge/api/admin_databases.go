package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

type adminDatabaseListResponse struct {
	Databases []*db.Database `json:"databases"`
}

func (s *Server) handleAdminListDatabases(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	if _, err := s.db.GetProjectByID(r.Context(), projectID); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	dbs, err := s.db.ListDatabasesByProject(r.Context(), projectID)
	if err != nil {
		logger.Error("admin/databases list", "error", err, "project_id", projectID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if dbs == nil {
		dbs = []*db.Database{}
	}
	s.writeJSON(w, http.StatusOK, adminDatabaseListResponse{Databases: dbs})
}

type adminCreateDatabaseRequest struct {
	ID string `json:"id"`
}

func (s *Server) handleAdminCreateDatabase(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	project, err := s.db.GetProjectByID(r.Context(), projectID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	var req adminCreateDatabaseRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	if err := db.ValidateDatabaseID(req.ID); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Reject duplicates with 409 instead of letting the unique-constraint
	// violation surface as a generic 500.
	if existing, err := s.db.GetDatabase(r.Context(), projectID, req.ID); err == nil && existing != nil {
		s.writeError(w, http.StatusConflict, "database already exists")
		return
	} else if err != nil && !errors.Is(err, db.ErrNotFound) {
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if err := s.db.CreateDatabase(r.Context(), projectID, req.ID, project.Ephemeral); err != nil {
		logger.Error("admin/databases create", "error", err, "project_id", projectID, "database_id", req.ID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	created, err := s.db.GetDatabase(r.Context(), projectID, req.ID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "created but couldn't reload")
		return
	}
	s.writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleAdminDeleteDatabase(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	dbID := r.PathValue("db_id")

	// Look up the current assignment so we can route the EVICT to the
	// correct backend after the row is gone.
	existing, err := s.db.GetDatabase(r.Context(), projectID, dbID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "database not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	serverID := existing.ServerID

	if err := s.db.EvictDatabases(r.Context(), []db.EvictionRequest{
		{ProjectID: projectID, DatabaseID: dbID},
	}); err != nil {
		logger.Error("admin/databases delete", "error", err, "project_id", projectID, "database_id", dbID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if s.notifyHandler != nil {
		// purge=true: persistent databases drop their on-disk WAL/blob.
		// Ephemeral databases have no on-disk state so the flag is a
		// no-op for them.
		s.notifyHandler.OnDatabaseEvicted(projectID, dbID, serverID, true)
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
