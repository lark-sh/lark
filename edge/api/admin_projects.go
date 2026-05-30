package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/jwt"
	"github.com/lark-sh/lark/edge/logger"
)

// defaultStarterRulesTTL is how long the auto-generated starter rules stay open
// before they lock down. Wide open for quick development, then deny everything so a project that gets spun
// up and forgotten doesn't stay world-readable/writable forever.
const defaultStarterRulesTTL = 14 * 24 * time.Hour

// defaultStarterRules builds the permissive-but-expiring rule set applied to a
// project when the caller doesn't supply their own. Every read and write is
// gated on `now < <expiry>`, where `now` is the server timestamp in
// milliseconds (matching the rules evaluator's `now`). Once the window passes
// the rules deny everything until the operator sets real rules.
func defaultStarterRules() string {
	expiry := time.Now().Add(defaultStarterRulesTTL).UnixMilli()
	return fmt.Sprintf(`{"rules":{".read":"now < %d",".write":"now < %d"}}`, expiry, expiry)
}

type adminProjectListResponse struct {
	Projects []*db.Project `json:"projects"`
}

func (s *Server) handleAdminListProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := s.db.ListProjects(r.Context())
	if err != nil {
		logger.Error("admin/projects list", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if projects == nil {
		projects = []*db.Project{}
	}
	s.writeJSON(w, http.StatusOK, adminProjectListResponse{Projects: projects})
}

func (s *Server) handleAdminGetProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := s.db.GetProjectByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, p)
}

type adminCreateProjectRequest struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	RulesJSON         string `json:"rules_json,omitempty"`
	Ephemeral         bool   `json:"ephemeral"`
	AutoCreate        bool   `json:"auto_create"`
	FirebaseProjectID string `json:"firebase_project_id,omitempty"`
}

func (s *Server) handleAdminCreateProject(w http.ResponseWriter, r *http.Request) {
	var req adminCreateProjectRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	req.Name = strings.TrimSpace(req.Name)
	if req.ID == "" || req.Name == "" {
		s.writeError(w, http.StatusBadRequest, "id and name are required")
		return
	}
	if err := db.ValidateDatabaseID(req.ID); err != nil {
		// Project IDs and database IDs share the same DNS-label rules.
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rules := req.RulesJSON
	if rules == "" {
		rules = defaultStarterRules()
	}

	project := &db.Project{
		ID:             req.ID,
		Name:           req.Name,
		SecretKey:      randomToken(16),
		AdminSecretKey: randomToken(16),
		RulesJSON:      rules,
		Ephemeral:      req.Ephemeral,
		AutoCreate:     req.AutoCreate,
		// Always on; no longer user-facing, but still plumbed through the DB/config
		// in case we want to reintroduce the toggle later.
		FirebaseCompatEnabled: true,
		FirebaseProjectID:     req.FirebaseProjectID,
	}
	if err := s.db.CreateProject(r.Context(), project); err != nil {
		logger.Error("admin/projects create", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusCreated, project)
}

type adminUpdateProjectRequest struct {
	Name                  *string `json:"name,omitempty"`
	RulesJSON             *string `json:"rules_json,omitempty"`
	Ephemeral             *bool   `json:"ephemeral,omitempty"`
	AutoCreate            *bool   `json:"auto_create,omitempty"`
	FirebaseCompatEnabled *bool   `json:"firebase_compat_enabled,omitempty"`
	FirebaseProjectID     *string `json:"firebase_project_id,omitempty"`
}

func (s *Server) handleAdminUpdateProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	project, err := s.db.GetProjectByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	var req adminUpdateProjectRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name != nil {
		project.Name = strings.TrimSpace(*req.Name)
	}
	if req.RulesJSON != nil {
		project.RulesJSON = *req.RulesJSON
	}
	if req.Ephemeral != nil {
		project.Ephemeral = *req.Ephemeral
	}
	if req.AutoCreate != nil {
		project.AutoCreate = *req.AutoCreate
	}
	if req.FirebaseCompatEnabled != nil {
		project.FirebaseCompatEnabled = *req.FirebaseCompatEnabled
	}
	if req.FirebaseProjectID != nil {
		project.FirebaseProjectID = *req.FirebaseProjectID
	}

	newVersion, err := s.db.UpdateProject(r.Context(), project)
	if err != nil {
		logger.Error("admin/projects update", "error", err, "project_id", id)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	project.ConfigVersion = newVersion

	if s.notifyHandler != nil {
		s.notifyHandler.OnProjectConfigChanged(id, newVersion)
	}
	s.writeJSON(w, http.StatusOK, project)
}

func (s *Server) handleAdminDeleteProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.db.DeleteProject(r.Context(), id); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		logger.Error("admin/projects delete", "error", err, "project_id", id)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// Databases referencing this project are removed via FK CASCADE.
	// Connected backends will discover the gap on their next routing
	// attempt; no explicit broadcast needed here.
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type adminRegenerateSecretResponse struct {
	SecretKey     string `json:"secret_key"`
	ConfigVersion int64  `json:"config_version"`
}

type adminAdminTokenResponse struct {
	Token string `json:"token"`
}

// handleAdminMintAdminToken issues a short-lived admin JWT scoped to a
// project. The dashboard's database editor uses this to authenticate to
// the wire-protocol endpoint on behalf of the logged-in operator.
func (s *Server) handleAdminMintAdminToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	account := AccountFromContext(r.Context())

	project, err := s.db.GetProjectByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	token, err := jwt.SignAdminToken(project.AdminSecretKey, account.ID)
	if err != nil {
		logger.Error("admin/admin-token sign", "error", err, "project_id", id, "account_id", account.ID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, adminAdminTokenResponse{Token: token})
}

func (s *Server) handleAdminRegenerateProjectSecret(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	newKey := randomToken(16)
	newVersion, err := s.db.RegenerateProjectSecret(r.Context(), id, newKey)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		logger.Error("admin/projects regenerate-secret", "error", err, "project_id", id)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if s.notifyHandler != nil {
		s.notifyHandler.OnProjectConfigChanged(id, newVersion)
	}
	s.writeJSON(w, http.StatusOK, adminRegenerateSecretResponse{
		SecretKey:     newKey,
		ConfigVersion: newVersion,
	})
}
