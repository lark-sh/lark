package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/lark-sh/lark/edge/db"
)

// BootstrapEmail is the email used for the first-boot admin account.
const BootstrapEmail = "admin@local"

// BootstrapProjectID / BootstrapProjectName name the default project
// created on first boot when no projects exist. Settings are aimed at
// the "I just want to play with this" path: time-limited starter rules
// (open now, locked down after defaultStarterRulesTTL), persistent (not
// ephemeral), and Firebase-compat on so existing Firebase SDK code can
// point at the subdomain and just work.
const (
	BootstrapProjectID   = "default"
	BootstrapProjectName = "Default"
)

// BootstrapAdminIfEmpty creates an initial admin account when the accounts
// table is empty. The new account has [db.Account.MustChangePassword]=true
// so the temporary password printed at boot can only be used once.
//
// Returns the freshly minted credentials, or empty strings if no bootstrap
// was needed (the table already has at least one account). main.go is
// responsible for logging the credentials in a way the operator can
// capture from boot output.
func BootstrapAdminIfEmpty(ctx context.Context, store db.Store) (email, password string, err error) {
	n, err := store.CountAccounts(ctx)
	if err != nil {
		return "", "", fmt.Errorf("count accounts: %w", err)
	}
	if n > 0 {
		return "", "", nil
	}

	// 12 random bytes → 24 hex chars. Comfortable to copy out of a
	// terminal and 96 bits of entropy in case it leaks via log
	// aggregation before the operator forces a reset.
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("rand: %w", err)
	}
	password = hex.EncodeToString(buf)

	hash, err := HashPassword(password)
	if err != nil {
		return "", "", fmt.Errorf("hash password: %w", err)
	}

	account := &db.Account{
		ID:                 NewAccountID(),
		Email:              BootstrapEmail,
		PasswordHash:       hash,
		Role:               "admin",
		MustChangePassword: true,
	}
	if err := store.CreateAccount(ctx, account); err != nil {
		return "", "", fmt.Errorf("create account: %w", err)
	}

	return BootstrapEmail, password, nil
}

// BootstrapDefaultProjectIfEmpty creates a "default" project when no
// projects exist. Returns true if a project was actually created. Idempotent.
//
// The default project is non-ephemeral, auto-creates databases on
// connect, has Firebase compat on, and ships time-limited starter rules
// (see defaultStarterRules) — the fastest "point a Firebase SDK at the
// subdomain and write something" path, without leaving the project open
// forever if it's forgotten. Operators who want different defaults can
// edit or delete this project via the dashboard.
func BootstrapDefaultProjectIfEmpty(ctx context.Context, store db.Store) (bool, error) {
	projects, err := store.ListProjects(ctx)
	if err != nil {
		return false, fmt.Errorf("list projects: %w", err)
	}
	if len(projects) > 0 {
		return false, nil
	}

	err = store.CreateProject(ctx, &db.Project{
		ID:                    BootstrapProjectID,
		Name:                  BootstrapProjectName,
		SecretKey:             randomToken(16),
		AdminSecretKey:        randomToken(16),
		RulesJSON:             defaultStarterRules(),
		Ephemeral:             false,
		AutoCreate:            true,
		FirebaseCompatEnabled: true,
	})
	if err != nil {
		return false, fmt.Errorf("create default project: %w", err)
	}
	return true, nil
}
