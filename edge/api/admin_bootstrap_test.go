package api

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lark-sh/lark/edge/db"
)

func openBootstrapStore(t *testing.T) db.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lark.db")
	store, err := db.NewSqlite(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestBootstrap_CreatesAccountWhenEmpty(t *testing.T) {
	store := openBootstrapStore(t)
	email, password, err := BootstrapAdminIfEmpty(context.Background(), store)
	if err != nil {
		t.Fatalf("BootstrapAdminIfEmpty: %v", err)
	}
	if email != BootstrapEmail {
		t.Errorf("email: got %q, want %q", email, BootstrapEmail)
	}
	if len(password) < 16 {
		t.Errorf("password too short: %q", password)
	}

	account, err := store.GetAccountByEmail(context.Background(), BootstrapEmail)
	if err != nil {
		t.Fatalf("GetAccountByEmail: %v", err)
	}
	if !account.MustChangePassword {
		t.Error("MustChangePassword: got false, want true")
	}
	if account.Role != "admin" {
		t.Errorf("Role: got %q, want admin", account.Role)
	}
	if !VerifyPassword(account.PasswordHash, password) {
		t.Error("returned password doesn't verify against stored hash")
	}
}

func TestBootstrap_NoOpWhenAccountsExist(t *testing.T) {
	store := openBootstrapStore(t)
	// Pre-seed an existing account so bootstrap should bail.
	hash, _ := HashPassword("existing-password")
	if err := store.CreateAccount(context.Background(), &db.Account{
		ID:           NewAccountID(),
		Email:        "someone@else.com",
		PasswordHash: hash,
		Role:         "admin",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	email, password, err := BootstrapAdminIfEmpty(context.Background(), store)
	if err != nil {
		t.Fatalf("BootstrapAdminIfEmpty: %v", err)
	}
	if email != "" || password != "" {
		t.Errorf("expected empty credentials, got email=%q password=%q", email, password)
	}

	// And admin@local must NOT have been created.
	if _, err := store.GetAccountByEmail(context.Background(), BootstrapEmail); err == nil {
		t.Error("admin@local was created even though the table wasn't empty")
	}
}

func TestBootstrap_GeneratedPasswordIsUnique(t *testing.T) {
	// Two separate stores → two independent bootstraps → two passwords.
	// They must differ.
	_, p1, err := BootstrapAdminIfEmpty(context.Background(), openBootstrapStore(t))
	if err != nil {
		t.Fatalf("bootstrap 1: %v", err)
	}
	_, p2, err := BootstrapAdminIfEmpty(context.Background(), openBootstrapStore(t))
	if err != nil {
		t.Fatalf("bootstrap 2: %v", err)
	}
	if p1 == p2 {
		t.Error("two bootstraps produced the same temporary password")
	}
}
