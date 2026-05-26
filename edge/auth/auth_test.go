package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// mintRS256Token signs a Firebase-ID-token-shaped JWT with a throwaway RSA key.
// The signature won't match Google's keys, but the fail-closed guards under test
// reject before signature verification, so it exercises exactly the bypass path.
func mintRS256Token(t *testing.T, issuer, aud, uid string, extra map[string]any) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	claims := jwt.MapClaims{
		"iss": issuer,
		"aud": aud,
		"sub": uid,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	for k, v := range extra {
		claims[k] = v
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "test-kid"
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	return s
}

// TestMultiValidatorRejectsFirebaseTokenWhenNoProjectConfigured covers security
// audit finding: a project with no Firebase project ID configured must reject
// Firebase ID tokens outright.
func TestMultiValidatorRejectsFirebaseTokenWhenNoProjectConfigured(t *testing.T) {
	const attackerProject = "attacker-project"
	token := mintRS256Token(t,
		"https://securetoken.google.com/"+attackerProject,
		attackerProject,
		"attacker-uid",
		map[string]any{"admin": true, "role": "gm"},
	)

	v := NewMultiValidator(nil) // no Firebase project IDs configured

	// The per-connection path: ValidateForProject with no firebase project ID.
	if _, err := v.ValidateForProject(token, "customer-secret", "admin-secret"); err == nil {
		t.Fatal("expected rejection of Firebase ID token when no Firebase project ID is configured")
	}

	// Same with an explicitly-empty firebase project ID argument.
	if _, err := v.ValidateForProject(token, "customer-secret", "admin-secret", ""); err == nil {
		t.Fatal("expected rejection with empty firebase project ID")
	}

	// Defense-in-depth: the firebase validator itself must reject an empty pin.
	if _, err := v.firebaseValidator.ValidateForProjectID(token, ""); err == nil {
		t.Fatal("ValidateForProjectID must reject an empty expected project ID")
	}
}

func TestGenerateAndValidateToken(t *testing.T) {
	secret := []byte("test-secret-key")
	uid := "user-123"
	provider := "google"
	claims := map[string]any{"role": "admin", "level": 5}

	// Generate token
	token, err := GenerateToken(secret, uid, provider, claims, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	// Validate token
	validator := NewValidator(secret)
	info, err := validator.Validate(token)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Check fields
	if info.UID != uid {
		t.Errorf("UID: got %q, want %q", info.UID, uid)
	}
	if info.Provider != provider {
		t.Errorf("Provider: got %q, want %q", info.Provider, provider)
	}
	if info.Token["role"] != "admin" {
		t.Errorf("Token[role]: got %v, want 'admin'", info.Token["role"])
	}
	if info.Token["level"] != float64(5) { // JSON numbers become float64
		t.Errorf("Token[level]: got %v, want 5", info.Token["level"])
	}
}

func TestValidateEmptyToken(t *testing.T) {
	validator := NewValidator([]byte("secret"))
	_, err := validator.Validate("")
	if err != ErrNoToken {
		t.Errorf("Expected ErrNoToken, got %v", err)
	}
}

func TestValidateExpiredToken(t *testing.T) {
	secret := []byte("test-secret")

	// Generate expired token
	token, _ := GenerateToken(secret, "user", "test", nil, -time.Hour)

	validator := NewValidator(secret)
	_, err := validator.Validate(token)
	if err != ErrExpiredToken {
		t.Errorf("Expected ErrExpiredToken, got %v", err)
	}
}

func TestValidateWrongSecret(t *testing.T) {
	token, _ := GenerateToken([]byte("secret1"), "user", "test", nil, time.Hour)

	validator := NewValidator([]byte("secret2"))
	_, err := validator.Validate(token)
	if err == nil {
		t.Error("Expected error for wrong secret")
	}
}

func TestValidateDefaultProvider(t *testing.T) {
	secret := []byte("test-secret")

	// Generate token without provider
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString(secret)

	validator := NewValidator(secret)
	info, err := validator.Validate(tokenString)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Provider should default to "custom"
	if info.Provider != "custom" {
		t.Errorf("Provider: got %q, want 'custom'", info.Provider)
	}
}

func TestMultiValidatorHS256CustomerToken(t *testing.T) {
	secretKey := "customer-secret"
	adminSecretKey := "admin-secret"

	// Create Lark customer token (has "uid" field)
	claims := &LarkCustomerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		UID:    "customer-user-123",
		Claims: map[string]any{"premium": true},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte(secretKey))

	validator := NewMultiValidator(nil)
	info, err := validator.ValidateForProject(tokenString, secretKey, adminSecretKey)
	if err != nil {
		t.Fatalf("ValidateForProject failed: %v", err)
	}

	if info.UID != "customer-user-123" {
		t.Errorf("UID: got %q, want 'customer-user-123'", info.UID)
	}
	if info.IsTrueAdmin {
		t.Error("Customer token should not be admin")
	}
	if info.Token["premium"] != true {
		t.Errorf("Token[premium]: got %v, want true", info.Token["premium"])
	}
}

func TestMultiValidatorHS256AdminToken(t *testing.T) {
	secretKey := "customer-secret"
	adminSecretKey := "admin-secret"

	// Create coordinator admin token (kid: "coordinator")
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "admin-user",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		Provider: "coordinator",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "coordinator"
	tokenString, _ := token.SignedString([]byte(adminSecretKey))

	validator := NewMultiValidator(nil)
	info, err := validator.ValidateForProject(tokenString, secretKey, adminSecretKey)
	if err != nil {
		t.Fatalf("ValidateForProject failed: %v", err)
	}

	if info.UID != "admin-user" {
		t.Errorf("UID: got %q, want 'admin-user'", info.UID)
	}
	if !info.IsTrueAdmin {
		t.Error("Coordinator token should be admin")
	}
}

func TestMultiValidatorLegacyToken(t *testing.T) {
	secretKey := "legacy-secret"

	// Create Firebase legacy token (has "d" field, no "uid")
	claims := &LegacyClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		Version: "0",
		D:       map[string]any{"custom_field": "value"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte(secretKey))

	validator := NewMultiValidator(nil)
	info, err := validator.ValidateForProject(tokenString, secretKey, "")
	if err != nil {
		t.Fatalf("ValidateForProject failed: %v", err)
	}

	// Legacy tokens have empty UID
	if info.UID != "" {
		t.Errorf("UID: got %q, want empty", info.UID)
	}
	if info.Provider != "custom" {
		t.Errorf("Provider: got %q, want 'custom'", info.Provider)
	}
	if info.Token["custom_field"] != "value" {
		t.Errorf("Token[custom_field]: got %v, want 'value'", info.Token["custom_field"])
	}
}

func TestMultiValidatorEmulatorMode(t *testing.T) {
	validator := NewMultiValidator(nil)

	// Without emulator mode, "owner" should fail
	_, err := validator.ValidateForProject("owner", "secret", "admin")
	if err != ErrInvalidToken {
		t.Errorf("Expected ErrInvalidToken for 'owner' without emulator mode, got %v", err)
	}

	// With emulator mode, "owner" should work
	validator.SetEmulatorMode(true)
	info, err := validator.ValidateForProject("owner", "secret", "admin")
	if err != nil {
		t.Fatalf("ValidateForProject failed with emulator mode: %v", err)
	}

	if info.UID != "owner" {
		t.Errorf("UID: got %q, want 'owner'", info.UID)
	}
	if !info.IsTrueAdmin {
		t.Error("Owner token should be admin")
	}
}

func TestMultiValidatorNoSecretKey(t *testing.T) {
	// Create a token signed with some secret
	claims := &LarkCustomerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		UID: "user",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("some-secret"))

	validator := NewMultiValidator(nil)

	// Empty secret key should fail
	_, err := validator.ValidateForProject(tokenString, "", "")
	if err == nil {
		t.Error("Expected error when no secret key available")
	}
}

func TestUserFriendlyError(t *testing.T) {
	tests := []struct {
		err      error
		contains string
	}{
		{ErrNoToken, "no token"},
		{ErrExpiredToken, "expired"},
		{ErrInvalidIssuer, "issuer"},
		{ErrInvalidAudience, "audience"},
		{ErrKeyNotFound, "key not found"},
		{ErrInvalidKeyID, "key ID"},
		{nil, ""},
	}

	for _, tt := range tests {
		result := UserFriendlyError(tt.err)
		if tt.contains != "" && len(result) == 0 {
			t.Errorf("UserFriendlyError(%v) returned empty string", tt.err)
		}
	}
}

func TestPeekTokenHeader(t *testing.T) {
	secret := []byte("test")

	// HS256 token
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "test-kid"
	tokenString, _ := token.SignedString(secret)

	alg, kid, err := peekTokenHeader(tokenString)
	if err != nil {
		t.Fatalf("peekTokenHeader failed: %v", err)
	}
	if alg != "HS256" {
		t.Errorf("alg: got %q, want 'HS256'", alg)
	}
	if kid != "test-kid" {
		t.Errorf("kid: got %q, want 'test-kid'", kid)
	}
}

func TestPeekTokenPayload(t *testing.T) {
	secret := []byte("test")

	claims := &LarkCustomerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "sub-value",
		},
		UID:    "uid-value",
		Claims: map[string]any{"foo": "bar"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString(secret)

	payload, err := peekTokenPayload(tokenString)
	if err != nil {
		t.Fatalf("peekTokenPayload failed: %v", err)
	}

	if payload["uid"] != "uid-value" {
		t.Errorf("uid: got %v, want 'uid-value'", payload["uid"])
	}
}

func TestFindRootClaims(t *testing.T) {
	payload := map[string]any{
		"iss":           "https://example.com",
		"sub":           "user-123",
		"aud":           "my-app",
		"exp":           float64(9999999999),
		"iat":           float64(1234567890),
		"custom_field":  "custom_value",
		"another_field": 42,
	}

	claims := findRootClaims(payload)

	// Reserved claims should be filtered out
	if _, ok := claims["iss"]; ok {
		t.Error("iss should be filtered out")
	}
	if _, ok := claims["sub"]; ok {
		t.Error("sub should be filtered out")
	}

	// Custom claims should remain
	if claims["custom_field"] != "custom_value" {
		t.Errorf("custom_field: got %v, want 'custom_value'", claims["custom_field"])
	}
	if claims["another_field"] != 42 {
		t.Errorf("another_field: got %v, want 42", claims["another_field"])
	}
}
