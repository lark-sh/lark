package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/db"
)

// newTestServer spins up an in-memory SQLite-backed api.Server with the
// admin API enabled. Returns the server, the store (for seeding), and a
// cleanup func.
func newTestServer(t *testing.T) (*Server, db.Store, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lark.db")
	store, err := db.NewSqlite(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	cfg := &config.Config{
		AdminAPIEnabled: true,
		DisableTLS:      true, // drops the Secure cookie flag so httptest can read it
	}
	s := New(cfg, store)
	return s, store, func() { store.Close() }
}

func seedAccount(t *testing.T, store db.Store, email, password string, mustChange bool) *db.Account {
	t.Helper()
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	a := &db.Account{
		ID:                 NewAccountID(),
		Email:              email,
		PasswordHash:       hash,
		Role:               "admin",
		MustChangePassword: mustChange,
	}
	if err := store.CreateAccount(context.Background(), a); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	return a
}

func postJSON(t *testing.T, s *Server, path string, body any, cookie *http.Cookie) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr.Result()
}

func sessionCookie(t *testing.T, resp *http.Response) *http.Cookie {
	t.Helper()
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookieName {
			return c
		}
	}
	t.Fatalf("no session cookie set in response")
	return nil
}

func TestHashPassword_RejectsShort(t *testing.T) {
	if _, err := HashPassword("short"); err == nil {
		t.Errorf("expected error for short password")
	}
}

func TestVerifyPassword(t *testing.T) {
	hash, _ := HashPassword("correct-horse-battery-staple")
	if !VerifyPassword(hash, "correct-horse-battery-staple") {
		t.Error("VerifyPassword: expected true for correct password")
	}
	if VerifyPassword(hash, "wrong-password") {
		t.Error("VerifyPassword: expected false for wrong password")
	}
}

func TestLogin_BadCredentials(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	seedAccount(t, store, "admin@local", "correct-password-1", false)

	resp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "wrong",
	}, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401", resp.StatusCode)
	}
}

func TestLogin_UnknownEmail(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	resp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "stranger@local",
		Password: "anything",
	}, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401", resp.StatusCode)
	}
}

func TestLogin_Success_SetsCookie(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	seedAccount(t, store, "admin@local", "correct-password-1", false)

	resp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "correct-password-1",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	cookie := sessionCookie(t, resp)
	if cookie.Value == "" {
		t.Error("session cookie value is empty")
	}
	if !cookie.HttpOnly {
		t.Error("session cookie should be HttpOnly")
	}
	if cookie.SameSite != http.SameSiteStrictMode {
		t.Errorf("session cookie SameSite: got %v, want Strict", cookie.SameSite)
	}

	var body adminLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Account == nil || body.Account.Email != "admin@local" {
		t.Errorf("response account: %+v", body.Account)
	}
	if body.Account != nil && body.Account.PasswordHash != "" {
		t.Error("password_hash leaked in login response")
	}
}

func TestMe_RequiresSession(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/me", nil)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401", rr.Code)
	}
}

func TestMe_ReturnsAccountWhenAuthenticated(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	seedAccount(t, store, "admin@local", "correct-password-1", false)

	resp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "correct-password-1",
	}, nil)
	cookie := sessionCookie(t, resp)

	req := httptest.NewRequest(http.MethodGet, "/admin/api/me", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rr.Code)
	}
	var got adminMeResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Account == nil || got.Account.Email != "admin@local" {
		t.Errorf("email: %+v", got.Account)
	}
}

func TestLogout_DeletesSession(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	seedAccount(t, s.db, "admin@local", "correct-password-1", false)

	loginResp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "correct-password-1",
	}, nil)
	cookie := sessionCookie(t, loginResp)

	// Logout
	logoutResp := postJSON(t, s, "/admin/api/logout", struct{}{}, cookie)
	if logoutResp.StatusCode != http.StatusOK {
		t.Errorf("logout status: got %d, want 200", logoutResp.StatusCode)
	}

	// The session must no longer authenticate /admin/me.
	req := httptest.NewRequest(http.MethodGet, "/admin/api/me", nil)
	req.AddCookie(cookie)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("post-logout /me status: got %d, want 401", rr.Code)
	}
}

func TestChangePassword_RequiresCurrentWhenNotForced(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	seedAccount(t, s.db, "admin@local", "correct-password-1", false)

	loginResp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "correct-password-1",
	}, nil)
	cookie := sessionCookie(t, loginResp)

	// Wrong current password → 401.
	wrong := postJSON(t, s, "/admin/api/change-password", adminChangePasswordRequest{
		CurrentPassword: "wrong",
		NewPassword:     "new-password-2",
	}, cookie)
	if wrong.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong current: got %d, want 401", wrong.StatusCode)
	}

	// Right current password → 200 + login with the new password works.
	right := postJSON(t, s, "/admin/api/change-password", adminChangePasswordRequest{
		CurrentPassword: "correct-password-1",
		NewPassword:     "new-password-2",
	}, cookie)
	if right.StatusCode != http.StatusOK {
		t.Fatalf("change-password: got %d, want 200", right.StatusCode)
	}

	relogin := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "new-password-2",
	}, nil)
	if relogin.StatusCode != http.StatusOK {
		t.Errorf("relogin with new password: got %d, want 200", relogin.StatusCode)
	}
}

func TestChangePassword_ForcedSkipsCurrentCheck(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	seedAccount(t, s.db, "admin@local", "temp-password-1", true /* mustChange */)

	loginResp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email:    "admin@local",
		Password: "temp-password-1",
	}, nil)
	cookie := sessionCookie(t, loginResp)

	// No current_password provided — accepted because MustChangePassword=true.
	resp := postJSON(t, s, "/admin/api/change-password", adminChangePasswordRequest{
		NewPassword: "real-password-1",
	}, cookie)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("forced change-password: got %d, want 200", resp.StatusCode)
	}
}

func TestAdminRoutes_DisabledByConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lark.db")
	store, err := db.NewSqlite(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	cfg := &config.Config{AdminAPIEnabled: false}
	s := New(cfg, store)

	resp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email: "x", Password: "y",
	}, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("with admin disabled: got %d, want 404", resp.StatusCode)
	}
}
