// Package jwt provides JWT token generation for the Lark proxy.
//
// # Overview
//
// This package creates signed JWTs for various authentication scenarios:
//   - Anonymous tokens: For clients that don't provide credentials
//   - Admin tokens: For testing and administrative access
//   - Custom tokens: For server-side authentication
//
// # Token Format
//
// Tokens use HS256 (HMAC-SHA256) signing with the project's secret key.
// The payload includes standard JWT claims plus Lark-specific fields:
//
//	{
//	  "sub": "user_id",           // Subject (user identifier)
//	  "aud": ["project/database"], // Audience (target database)
//	  "exp": 1234567890,          // Expiration timestamp
//	  "iat": 1234567890,          // Issued-at timestamp
//	  "server": "coordinator",    // Issuing server
//	  "project": "project_id",    // Project ID
//	  "provider": "anonymous",    // Auth provider
//	  "isAdmin": false,           // Admin flag
//	  "claims": {}                // Custom claims
//	}
//
// # Security Notes
//
// The secret key must be kept secure. It's stored in the project record and
// should never be exposed to clients. Tokens are validated by the auth package
// using the same secret.
//
// # Relationship to auth Package
//
// This package creates tokens; the auth package validates them. They share
// the same Claims structure to ensure compatibility.
package jwt

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

// Claims represents the JWT claims for Lark tokens
type Claims struct {
	jwt.RegisteredClaims
	Server   string                 `json:"server,omitempty"`
	Project  string                 `json:"project,omitempty"`
	Provider string                 `json:"provider,omitempty"`
	IsAdmin  bool                   `json:"isAdmin,omitempty"`
	Custom   map[string]interface{} `json:"claims,omitempty"`
}

// SignAnonymousToken creates a token for anonymous users
func SignAnonymousToken(serverSecret, projectID, databaseID string) (string, error) {
	now := time.Now()
	anonID := "anon_" + randomHex(8)

	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   anonID,
			Audience:  jwt.ClaimStrings{projectID + "/" + databaseID},
			ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
		Server:   "coordinator",
		Project:  projectID,
		Provider: "anonymous",
		Custom: map[string]interface{}{
			"isAnonymous": true,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(serverSecret))
}

// SignCustomToken re-signs a custom token with the server secret
func SignCustomToken(serverSecret, projectID, databaseID, subject string, customClaims map[string]interface{}) (string, error) {
	now := time.Now()

	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			Audience:  jwt.ClaimStrings{projectID + "/" + databaseID},
			ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
		Server:   "coordinator",
		Project:  projectID,
		Provider: "custom",
		Custom:   customClaims,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(serverSecret))
}

// SignAdminToken creates an admin token signed with the admin secret key
func SignAdminToken(adminSecretKey, accountID string) (string, error) {
	now := time.Now()

	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   accountID,
			ExpiresAt: jwt.NewNumericDate(now.Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
		IsAdmin: true,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	// Set kid header to indicate this uses admin_secret_key
	token.Header["kid"] = "coordinator"
	return token.SignedString([]byte(adminSecretKey))
}

// VerifyToken verifies a token signed with the given secret
func VerifyToken(tokenString, secret string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}
		return []byte(secret), nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}

// VerifyCustomToken verifies a custom token from a client (signed with project secret)
func VerifyCustomToken(tokenString, projectSecret string) (*Claims, error) {
	return VerifyToken(tokenString, projectSecret)
}

// randomHex generates a random hex string of n bytes.
// Panics if the system RNG is unavailable — silently producing zero bytes
// here would collapse all anonymous user IDs to the same predictable value.
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
