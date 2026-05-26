package jwt

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testSecret      = "test-secret-key-12345"
	testAdminSecret = "admin-secret-key-12345"
	testProjectID   = "test-project"
	testDatabaseID  = "test-database"
)

func TestSignAnonymousToken(t *testing.T) {
	token, err := SignAnonymousToken(testSecret, testProjectID, testDatabaseID)
	if err != nil {
		t.Fatalf("SignAnonymousToken failed: %v", err)
	}

	// Verify it's a valid JWT
	if !strings.HasPrefix(token, "eyJ") {
		t.Error("token should be a valid JWT starting with eyJ")
	}

	// Verify the token
	claims, err := VerifyToken(token, testSecret)
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}

	// Check claims
	if !strings.HasPrefix(claims.Subject, "anon_") {
		t.Errorf("Subject should start with 'anon_', got %q", claims.Subject)
	}
	if claims.Provider != "anonymous" {
		t.Errorf("Provider: got %q, want %q", claims.Provider, "anonymous")
	}
	if claims.Server != "coordinator" {
		t.Errorf("Server: got %q, want %q", claims.Server, "coordinator")
	}
	if claims.Project != testProjectID {
		t.Errorf("Project: got %q, want %q", claims.Project, testProjectID)
	}
	if claims.Custom["isAnonymous"] != true {
		t.Error("Custom.isAnonymous should be true")
	}

	// Check audience
	expectedAud := testProjectID + "/" + testDatabaseID
	if len(claims.Audience) != 1 || claims.Audience[0] != expectedAud {
		t.Errorf("Audience: got %v, want [%s]", claims.Audience, expectedAud)
	}
}

func TestSignCustomToken(t *testing.T) {
	subject := "user_12345"
	customClaims := map[string]interface{}{
		"role":  "admin",
		"level": float64(10), // JSON numbers are float64
	}

	token, err := SignCustomToken(testSecret, testProjectID, testDatabaseID, subject, customClaims)
	if err != nil {
		t.Fatalf("SignCustomToken failed: %v", err)
	}

	// Verify the token
	claims, err := VerifyToken(token, testSecret)
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}

	// Check claims
	if claims.Subject != subject {
		t.Errorf("Subject: got %q, want %q", claims.Subject, subject)
	}
	if claims.Provider != "custom" {
		t.Errorf("Provider: got %q, want %q", claims.Provider, "custom")
	}
	if claims.Custom["role"] != "admin" {
		t.Errorf("Custom.role: got %v, want %q", claims.Custom["role"], "admin")
	}
	if claims.Custom["level"] != float64(10) {
		t.Errorf("Custom.level: got %v, want %v", claims.Custom["level"], float64(10))
	}
}

func TestSignAdminToken(t *testing.T) {
	accountID := "account_12345"

	token, err := SignAdminToken(testAdminSecret, accountID)
	if err != nil {
		t.Fatalf("SignAdminToken failed: %v", err)
	}

	// Parse the token manually to check headers
	parsed, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
		return []byte(testAdminSecret), nil
	})
	if err != nil {
		t.Fatalf("jwt.Parse failed: %v", err)
	}

	// Check kid header
	kid, ok := parsed.Header["kid"].(string)
	if !ok || kid != "coordinator" {
		t.Errorf("Header.kid: got %v, want %q", parsed.Header["kid"], "coordinator")
	}

	// Verify the token
	claims, err := VerifyToken(token, testAdminSecret)
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}

	// Check claims
	if claims.Subject != accountID {
		t.Errorf("Subject: got %q, want %q", claims.Subject, accountID)
	}
	if !claims.IsAdmin {
		t.Error("IsAdmin should be true")
	}
}

func TestAdminTokenExpiration(t *testing.T) {
	token, err := SignAdminToken(testAdminSecret, "test-account")
	if err != nil {
		t.Fatalf("SignAdminToken failed: %v", err)
	}

	claims, err := VerifyToken(token, testAdminSecret)
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}

	// Admin tokens should expire in 1 hour
	expiresIn := time.Until(claims.ExpiresAt.Time)
	if expiresIn < 59*time.Minute || expiresIn > 61*time.Minute {
		t.Errorf("Admin token should expire in ~1 hour, got %v", expiresIn)
	}
}

func TestAnonymousTokenExpiration(t *testing.T) {
	token, err := SignAnonymousToken(testSecret, testProjectID, testDatabaseID)
	if err != nil {
		t.Fatalf("SignAnonymousToken failed: %v", err)
	}

	claims, err := VerifyToken(token, testSecret)
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}

	// Anonymous tokens should expire in 24 hours
	expiresIn := time.Until(claims.ExpiresAt.Time)
	if expiresIn < 23*time.Hour || expiresIn > 25*time.Hour {
		t.Errorf("Anonymous token should expire in ~24 hours, got %v", expiresIn)
	}
}

func TestVerifyTokenInvalidSignature(t *testing.T) {
	token, err := SignAnonymousToken(testSecret, testProjectID, testDatabaseID)
	if err != nil {
		t.Fatalf("SignAnonymousToken failed: %v", err)
	}

	// Try to verify with wrong secret
	_, err = VerifyToken(token, "wrong-secret")
	if err != ErrInvalidToken {
		t.Errorf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerifyTokenMalformed(t *testing.T) {
	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"garbage", "not-a-jwt"},
		{"incomplete", "eyJ.abc"},
		{"wrong parts", "a.b.c.d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := VerifyToken(tt.token, testSecret)
			if err != ErrInvalidToken {
				t.Errorf("expected ErrInvalidToken, got %v", err)
			}
		})
	}
}

func TestVerifyTokenExpired(t *testing.T) {
	// Create a token that's already expired
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "test-user",
			ExpiresAt: jwt.NewNumericDate(now.Add(-1 * time.Hour)), // expired 1 hour ago
			IssuedAt:  jwt.NewNumericDate(now.Add(-2 * time.Hour)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("Failed to create expired token: %v", err)
	}

	_, err = VerifyToken(tokenString, testSecret)
	if err != ErrExpiredToken {
		t.Errorf("expected ErrExpiredToken, got %v", err)
	}
}

func TestVerifyCustomToken(t *testing.T) {
	// VerifyCustomToken is just an alias for VerifyToken
	token, err := SignCustomToken(testSecret, testProjectID, testDatabaseID, "user", nil)
	if err != nil {
		t.Fatalf("SignCustomToken failed: %v", err)
	}

	claims, err := VerifyCustomToken(token, testSecret)
	if err != nil {
		t.Fatalf("VerifyCustomToken failed: %v", err)
	}

	if claims.Subject != "user" {
		t.Errorf("Subject: got %q, want %q", claims.Subject, "user")
	}
}

func TestSignTokensAreUnique(t *testing.T) {
	// Each anonymous token should have a unique subject
	token1, _ := SignAnonymousToken(testSecret, testProjectID, testDatabaseID)
	token2, _ := SignAnonymousToken(testSecret, testProjectID, testDatabaseID)

	if token1 == token2 {
		t.Error("Anonymous tokens should be unique")
	}

	claims1, _ := VerifyToken(token1, testSecret)
	claims2, _ := VerifyToken(token2, testSecret)

	if claims1.Subject == claims2.Subject {
		t.Error("Anonymous token subjects should be unique")
	}
}

func TestSigningMethodIsHMAC(t *testing.T) {
	token, err := SignAnonymousToken(testSecret, testProjectID, testDatabaseID)
	if err != nil {
		t.Fatalf("SignAnonymousToken failed: %v", err)
	}

	// Parse without verification to check the algorithm
	parsed, _, err := new(jwt.Parser).ParseUnverified(token, &Claims{})
	if err != nil {
		t.Fatalf("ParseUnverified failed: %v", err)
	}

	if parsed.Method.Alg() != "HS256" {
		t.Errorf("Expected HS256 algorithm, got %s", parsed.Method.Alg())
	}
}
