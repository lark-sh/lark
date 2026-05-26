package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lark-sh/lark/edge/db"
)

// loggedIn returns a cookie that authenticates an admin user with the
// given email/password. The account is created fresh.
func loggedIn(t *testing.T, s *Server, email, password string) *http.Cookie {
	t.Helper()
	seedAccount(t, s.db, email, password, false)
	resp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email: email, Password: password,
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login: got %d", resp.StatusCode)
	}
	return sessionCookie(t, resp)
}

func getJSON(t *testing.T, s *Server, path string, cookie *http.Cookie, into any) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if into != nil && rr.Code/100 == 2 {
		if err := json.Unmarshal(rr.Body.Bytes(), into); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
	return rr.Code
}

func reqJSON(t *testing.T, s *Server, method, path string, body any, cookie *http.Cookie, into any) int {
	t.Helper()
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(buf)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if into != nil && rr.Code/100 == 2 {
		if err := json.Unmarshal(rr.Body.Bytes(), into); err != nil {
			t.Fatalf("decode %s %s: %v", method, path, err)
		}
	}
	return rr.Code
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

func TestUsers_CreateAndList(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	var created adminCreateUserResponse
	if code := reqJSON(t, s, http.MethodPost, "/admin/api/users",
		adminCreateUserRequest{Email: "bob@local"}, cookie, &created); code != http.StatusCreated {
		t.Fatalf("create: got %d", code)
	}
	if created.Account.Email != "bob@local" {
		t.Errorf("email: got %q", created.Account.Email)
	}
	if !created.Account.MustChangePassword {
		t.Error("must_change_password should default to true on admin create")
	}
	if len(created.TemporaryPassword) < 16 {
		t.Errorf("temp password too short: %q", created.TemporaryPassword)
	}

	var list adminUserListResponse
	if code := getJSON(t, s, "/admin/api/users", cookie, &list); code != http.StatusOK {
		t.Fatalf("list: got %d", code)
	}
	if len(list.Users) != 2 {
		t.Errorf("user count: got %d, want 2", len(list.Users))
	}
}

func TestUsers_DuplicateEmailRejected(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	reqJSON(t, s, http.MethodPost, "/admin/api/users",
		adminCreateUserRequest{Email: "bob@local"}, cookie, nil)
	code := reqJSON(t, s, http.MethodPost, "/admin/api/users",
		adminCreateUserRequest{Email: "bob@local"}, cookie, nil)
	if code != http.StatusConflict {
		t.Errorf("duplicate: got %d, want 409", code)
	}
}

func TestUsers_CannotDeleteSelf(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	self, _ := store.GetAccountByEmail(context.Background(), "admin@local")
	code := reqJSON(t, s, http.MethodDelete, "/admin/api/users/"+self.ID, nil, cookie, nil)
	if code != http.StatusBadRequest {
		t.Errorf("self-delete: got %d, want 400", code)
	}
}

func TestUsers_DeleteOther(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	var created adminCreateUserResponse
	reqJSON(t, s, http.MethodPost, "/admin/api/users",
		adminCreateUserRequest{Email: "bob@local"}, cookie, &created)

	code := reqJSON(t, s, http.MethodDelete, "/admin/api/users/"+created.Account.ID, nil, cookie, nil)
	if code != http.StatusOK {
		t.Fatalf("delete: got %d", code)
	}
	if _, err := store.GetAccountByEmail(context.Background(), "bob@local"); err == nil {
		t.Error("account still present after delete")
	}
}

func TestUsers_ResetPassword(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	var created adminCreateUserResponse
	reqJSON(t, s, http.MethodPost, "/admin/api/users",
		adminCreateUserRequest{Email: "bob@local"}, cookie, &created)

	var reset adminResetPasswordResponse
	code := reqJSON(t, s, http.MethodPost, "/admin/api/users/"+created.Account.ID+"/reset-password",
		nil, cookie, &reset)
	if code != http.StatusOK {
		t.Fatalf("reset: got %d", code)
	}
	if reset.TemporaryPassword == created.TemporaryPassword {
		t.Error("reset returned the same temp password as create")
	}
	// Bob can now log in with the new temp password.
	loginResp := postJSON(t, s, "/admin/api/login", adminLoginRequest{
		Email: "bob@local", Password: reset.TemporaryPassword,
	}, nil)
	if loginResp.StatusCode != http.StatusOK {
		t.Errorf("login with reset password: got %d", loginResp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

func TestProjects_CreateListGetUpdateDelete(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	// Create.
	var created db.Project
	if code := reqJSON(t, s, http.MethodPost, "/admin/api/projects", adminCreateProjectRequest{
		ID:         "my-app",
		Name:       "My App",
		AutoCreate: true,
		Ephemeral:  false,
	}, cookie, &created); code != http.StatusCreated {
		t.Fatalf("create: got %d", code)
	}
	if created.ID != "my-app" || created.SecretKey == "" {
		t.Errorf("created: %+v", created)
	}

	// List.
	var listed adminProjectListResponse
	getJSON(t, s, "/admin/api/projects", cookie, &listed)
	if len(listed.Projects) != 1 || listed.Projects[0].ID != "my-app" {
		t.Errorf("list: %+v", listed)
	}

	// Get.
	var got db.Project
	if code := getJSON(t, s, "/admin/api/projects/my-app", cookie, &got); code != http.StatusOK {
		t.Fatalf("get: %d", code)
	}
	if got.Name != "My App" {
		t.Errorf("name: %q", got.Name)
	}

	// Update.
	newName := "Renamed App"
	autoCreate := false
	var updated db.Project
	if code := reqJSON(t, s, http.MethodPatch, "/admin/api/projects/my-app", adminUpdateProjectRequest{
		Name:       &newName,
		AutoCreate: &autoCreate,
	}, cookie, &updated); code != http.StatusOK {
		t.Fatalf("update: got %d", code)
	}
	if updated.Name != "Renamed App" || updated.AutoCreate || updated.ConfigVersion != created.ConfigVersion+1 {
		t.Errorf("after update: %+v", updated)
	}

	// Delete.
	if code := reqJSON(t, s, http.MethodDelete, "/admin/api/projects/my-app", nil, cookie, nil); code != http.StatusOK {
		t.Errorf("delete: %d", code)
	}
	if code := getJSON(t, s, "/admin/api/projects/my-app", cookie, nil); code != http.StatusNotFound {
		t.Errorf("post-delete get: got %d, want 404", code)
	}
}

func TestProjects_RegenerateSecret(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	var created db.Project
	reqJSON(t, s, http.MethodPost, "/admin/api/projects",
		adminCreateProjectRequest{ID: "my-app", Name: "App"}, cookie, &created)
	oldSecret := created.SecretKey

	var regen adminRegenerateSecretResponse
	if code := reqJSON(t, s, http.MethodPost, "/admin/api/projects/my-app/regenerate-secret",
		nil, cookie, &regen); code != http.StatusOK {
		t.Fatalf("regenerate: got %d", code)
	}
	if regen.SecretKey == "" || regen.SecretKey == oldSecret {
		t.Errorf("secret unchanged: %q", regen.SecretKey)
	}
	if regen.ConfigVersion <= created.ConfigVersion {
		t.Errorf("config_version not bumped: got %d, want > %d", regen.ConfigVersion, created.ConfigVersion)
	}
}

func TestProjects_BadIDRejected(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")

	code := reqJSON(t, s, http.MethodPost, "/admin/api/projects",
		adminCreateProjectRequest{ID: "Bad ID!", Name: "x"}, cookie, nil)
	if code != http.StatusBadRequest {
		t.Errorf("got %d, want 400", code)
	}
}

func TestProjects_MintAdminToken(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	reqJSON(t, s, http.MethodPost, "/admin/api/projects",
		adminCreateProjectRequest{ID: "p1", Name: "P1"}, cookie, nil)

	var tok struct {
		Token string `json:"token"`
	}
	if code := reqJSON(t, s, http.MethodPost, "/admin/api/projects/p1/admin-token",
		nil, cookie, &tok); code != http.StatusOK {
		t.Fatalf("mint: got %d", code)
	}
	if tok.Token == "" {
		t.Error("empty token")
	}
	// JWTs are three base64 segments separated by dots.
	if dots := 0; dots < 2 {
		for _, c := range tok.Token {
			if c == '.' {
				dots++
			}
		}
		if dots != 2 {
			t.Errorf("not JWT-shaped: %q", tok.Token)
		}
	}
}

func TestProjects_MintAdminTokenRequiresSession(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	if err := store.CreateProject(context.Background(), &db.Project{
		ID: "p1", Name: "P1", SecretKey: "sk", AdminSecretKey: "ask", AutoCreate: true,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	code := reqJSON(t, s, http.MethodPost, "/admin/api/projects/p1/admin-token",
		nil, nil, nil)
	if code != http.StatusUnauthorized {
		t.Errorf("got %d, want 401", code)
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

func TestStats_ReturnsCounts(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	reqJSON(t, s, http.MethodPost, "/admin/api/projects",
		adminCreateProjectRequest{ID: "p1", Name: "P1"}, cookie, nil)
	reqJSON(t, s, http.MethodPost, "/admin/api/users",
		adminCreateUserRequest{Email: "bob@local"}, cookie, nil)

	var stats adminStatsResponse
	if code := getJSON(t, s, "/admin/api/stats", cookie, &stats); code != http.StatusOK {
		t.Fatalf("stats: %d", code)
	}
	if stats.Accounts != 2 {
		t.Errorf("accounts: got %d, want 2", stats.Accounts)
	}
	if stats.Projects != 1 {
		t.Errorf("projects: got %d, want 1", stats.Projects)
	}
}
