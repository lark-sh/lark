package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"

	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookieName = "lark_session"
	sessionTTL        = 30 * 24 * time.Hour
	bcryptCost        = bcrypt.DefaultCost
)

// HashPassword returns the bcrypt hash of the given plaintext password.
// Exported so the first-boot bootstrap (in main.go) can use it.
func HashPassword(plaintext string) (string, error) {
	if len(plaintext) < 8 {
		return "", errors.New("password must be at least 8 characters")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(h), nil
}

// VerifyPassword reports whether the plaintext matches the stored hash.
func VerifyPassword(hash, plaintext string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plaintext)) == nil
}

// dummyPasswordHash is a valid bcrypt hash of a random, discarded password. On
// the unknown-email login path we compare against it so that path costs the same
// bcrypt work as a wrong-password-for-a-real-account path — otherwise an attacker
// could tell which emails are registered by response latency (audit L-1).
var dummyPasswordHash = func() string {
	h, err := bcrypt.GenerateFromPassword([]byte(randomToken(16)), bcryptCost)
	if err != nil {
		// bcrypt only errors on a broken build/runtime; fail loudly at startup.
		panic(err)
	}
	return string(h)
}()

// randomToken returns n random bytes, hex-encoded. Used for session IDs
// (256 bits) and opaque account/session public IDs (128 bits).
func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// rand.Read only fails when the OS entropy source is broken,
		// which is unrecoverable.
		panic(err)
	}
	return hex.EncodeToString(b)
}

// NewAccountID generates a fresh opaque account ID.
func NewAccountID() string { return randomToken(16) }

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// registerAdminRoutes wires the /admin/api endpoints onto the mux. Only
// called when ADMIN_API_ENABLED=true. The SPA mounts at /admin/* (see
// mountAdminSPA); this leaves the API surface under /admin/api/* so they
// don't collide on overlapping paths like /admin/projects/{id}.
//
// Routes that read or mutate authenticated state run through
// requireSession; login/logout do not.
func (s *Server) registerAdminRoutes() {
	// Auth.
	s.mux.HandleFunc("POST /admin/api/login", s.handleAdminLogin)
	s.mux.HandleFunc("POST /admin/api/logout", s.handleAdminLogout)
	s.mux.HandleFunc("POST /admin/api/change-password", s.requireSession(s.handleAdminChangePassword))
	s.mux.HandleFunc("GET /admin/api/me", s.requireSession(s.handleAdminMe))

	// Users.
	s.mux.HandleFunc("GET /admin/api/users", s.requireSession(s.handleAdminListUsers))
	s.mux.HandleFunc("POST /admin/api/users", s.requireSession(s.handleAdminCreateUser))
	s.mux.HandleFunc("DELETE /admin/api/users/{id}", s.requireSession(s.handleAdminDeleteUser))
	s.mux.HandleFunc("POST /admin/api/users/{id}/reset-password", s.requireSession(s.handleAdminResetUserPassword))

	// Projects.
	s.mux.HandleFunc("GET /admin/api/projects", s.requireSession(s.handleAdminListProjects))
	s.mux.HandleFunc("GET /admin/api/projects/{id}", s.requireSession(s.handleAdminGetProject))
	s.mux.HandleFunc("POST /admin/api/projects", s.requireSession(s.handleAdminCreateProject))
	s.mux.HandleFunc("PATCH /admin/api/projects/{id}", s.requireSession(s.handleAdminUpdateProject))
	s.mux.HandleFunc("DELETE /admin/api/projects/{id}", s.requireSession(s.handleAdminDeleteProject))
	s.mux.HandleFunc("POST /admin/api/projects/{id}/regenerate-secret", s.requireSession(s.handleAdminRegenerateProjectSecret))
	s.mux.HandleFunc("POST /admin/api/projects/{id}/admin-token", s.requireSession(s.handleAdminMintAdminToken))

	// Databases (scoped under a project).
	s.mux.HandleFunc("GET /admin/api/projects/{id}/databases", s.requireSession(s.handleAdminListDatabases))
	s.mux.HandleFunc("POST /admin/api/projects/{id}/databases", s.requireSession(s.handleAdminCreateDatabase))
	s.mux.HandleFunc("DELETE /admin/api/projects/{id}/databases/{db_id}", s.requireSession(s.handleAdminDeleteDatabase))

	// Metrics + events.
	s.mux.HandleFunc("GET /admin/api/projects/{id}/dashboard", s.requireSession(s.handleAdminProjectDashboard))
	s.mux.HandleFunc("GET /admin/api/projects/{id}/events", s.requireSession(s.handleAdminProjectEvents))

	// Stats.
	s.mux.HandleFunc("GET /admin/api/stats", s.requireSession(s.handleAdminStats))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type adminLoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type adminLoginResponse struct {
	Account *db.Account `json:"account"`
}

func (s *Server) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	var req adminLoginRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" || req.Password == "" {
		s.writeError(w, http.StatusBadRequest, "email and password are required")
		return
	}

	account, err := s.db.GetAccountByEmail(r.Context(), req.Email)
	// Always run one bcrypt comparison, whether or not the email exists, so the
	// unknown-email and wrong-password paths are indistinguishable by timing
	// (audit L-1). For an unknown email we compare against a throwaway hash.
	var authed bool
	if err != nil {
		VerifyPassword(dummyPasswordHash, req.Password)
	} else {
		authed = VerifyPassword(account.PasswordHash, req.Password)
	}
	if !authed {
		// Record the failure and apply per-account backoff before responding, so
		// repeated guesses against an account are throttled (audit L-2). Only
		// failures are delayed — a correct password always returns promptly, so a
		// legitimate admin is never locked out even while their email is attacked.
		if delay := s.loginThrottle.fail(req.Email); delay > 0 {
			time.Sleep(delay)
		}
		// Identical response for unknown email and wrong password (no enumeration).
		s.writeError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	s.loginThrottle.reset(req.Email)

	now := db.NowMS()
	session := &db.Session{
		ID:               randomToken(32),
		PublicID:         randomToken(16),
		AccountID:        account.ID,
		Kind:             "dashboard",
		CreatedIP:        clientIP(r),
		CreatedUserAgent: r.UserAgent(),
		ExpiresAt:        now + sessionTTL.Milliseconds(),
		CreatedAt:        now,
	}
	if err := s.db.CreateSession(r.Context(), session); err != nil {
		logger.Error("admin/login: create session", "error", err, "account_id", account.ID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.setSessionCookie(w, session.ID, sessionTTL)
	s.writeJSON(w, http.StatusOK, adminLoginResponse{Account: account})
}

func (s *Server) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookieName); err == nil && cookie.Value != "" {
		if err := s.db.DeleteSession(r.Context(), cookie.Value); err != nil {
			// Log but don't fail — clearing the cookie still effectively
			// logs the user out.
			logger.Warn("admin/logout: delete session", "error", err)
		}
	}
	s.clearSessionCookie(w)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type adminChangePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

func (s *Server) handleAdminChangePassword(w http.ResponseWriter, r *http.Request) {
	account := AccountFromContext(r.Context())

	var req adminChangePasswordRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	// If the account isn't flagged for forced reset, require the current
	// password. The forced-reset path (first-boot bootstrap, admin reset)
	// is the only way to skip the old-password check.
	if !account.MustChangePassword {
		if !VerifyPassword(account.PasswordHash, req.CurrentPassword) {
			s.writeError(w, http.StatusUnauthorized, "current password is incorrect")
			return
		}
	}

	hash, err := HashPassword(req.NewPassword)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.db.UpdateAccountPassword(r.Context(), account.ID, hash, false); err != nil {
		logger.Error("admin/change-password: update", "error", err, "account_id", account.ID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// adminMeResponse bundles the authenticated account with the bits of
// per-deployment config the SPA needs at runtime — chiefly LARKDB_DOMAIN,
// which the database editor uses to build subdomain URLs into the wire
// protocol on the operator's actual domain.
type adminMeResponse struct {
	Account      *db.Account `json:"account"`
	LarkDBDomain string      `json:"larkdb_domain"`
}

func (s *Server) handleAdminMe(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, adminMeResponse{
		Account:      AccountFromContext(r.Context()),
		LarkDBDomain: s.config.LarkDBDomain,
	})
}

// clientIP returns the request's source IP, preferring X-Forwarded-For when
// the proxy fronting lark-edge provides one. Falls back to RemoteAddr.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.Index(xff, ","); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if host, _, ok := strings.Cut(r.RemoteAddr, ":"); ok {
		return host
	}
	return r.RemoteAddr
}
