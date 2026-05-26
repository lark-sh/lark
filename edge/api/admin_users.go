package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

// randomPassword returns a 24-character hex string (96 bits of entropy)
// suitable for use as a one-time-use temporary password.
func randomPassword() (string, error) {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

type adminUserListResponse struct {
	Users []*db.Account `json:"users"`
}

func (s *Server) handleAdminListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.db.ListAccounts(r.Context())
	if err != nil {
		logger.Error("admin/users list", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if users == nil {
		users = []*db.Account{}
	}
	s.writeJSON(w, http.StatusOK, adminUserListResponse{Users: users})
}

type adminCreateUserRequest struct {
	Email string `json:"email"`
}

type adminCreateUserResponse struct {
	Account           *db.Account `json:"account"`
	TemporaryPassword string      `json:"temporary_password"`
}

func (s *Server) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	var req adminCreateUserRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" {
		s.writeError(w, http.StatusBadRequest, "email is required")
		return
	}

	// Reject if an account with this email already exists. The schema's
	// UNIQUE constraint would catch this too, but a clean 409 is friendlier
	// than a generic 500.
	if existing, err := s.db.GetAccountByEmail(r.Context(), req.Email); err == nil && existing != nil {
		s.writeError(w, http.StatusConflict, "email already in use")
		return
	} else if err != nil && !errors.Is(err, db.ErrNotFound) {
		logger.Error("admin/users create lookup", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	tempPassword, err := randomPassword()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	hash, err := HashPassword(tempPassword)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	account := &db.Account{
		ID:                 NewAccountID(),
		Email:              req.Email,
		PasswordHash:       hash,
		Role:               "admin",
		MustChangePassword: true,
	}
	if err := s.db.CreateAccount(r.Context(), account); err != nil {
		logger.Error("admin/users create", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.writeJSON(w, http.StatusCreated, adminCreateUserResponse{
		Account:           account,
		TemporaryPassword: tempPassword,
	})
}

func (s *Server) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	self := AccountFromContext(r.Context())

	if id == self.ID {
		s.writeError(w, http.StatusBadRequest, "cannot delete your own account")
		return
	}
	if _, err := s.db.GetAccountByID(r.Context(), id); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "user not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := s.db.DeleteAccount(r.Context(), id); err != nil {
		logger.Error("admin/users delete", "error", err, "user_id", id)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type adminResetPasswordResponse struct {
	TemporaryPassword string `json:"temporary_password"`
}

func (s *Server) handleAdminResetUserPassword(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if _, err := s.db.GetAccountByID(r.Context(), id); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "user not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	tempPassword, err := randomPassword()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	hash, err := HashPassword(tempPassword)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := s.db.UpdateAccountPassword(r.Context(), id, hash, true); err != nil {
		logger.Error("admin/users reset-password", "error", err, "user_id", id)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, adminResetPasswordResponse{TemporaryPassword: tempPassword})
}
