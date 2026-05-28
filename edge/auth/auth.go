// Package auth provides authentication validators for the Lark proxy.
//
// # Overview
//
// This package validates authentication tokens from clients connecting to Lark databases.
// It supports multiple token formats to maintain compatibility with different client SDKs:
//
//   - Lark tokens: Custom JWTs signed with the project's secret key
//   - Firebase ID tokens: Standard Firebase Auth ID tokens (RS256, Google-signed)
//   - Firebase custom tokens: Service account tokens for server-side auth
//   - Anonymous tokens: Auto-generated tokens for unauthenticated access
//
// # Token Validation Flow
//
// 1. Client sends auth message with token
// 2. MultiValidator tries each validator in order (Lark → Firebase ID → Firebase Custom)
// 3. First successful validation returns auth.Info with user identity and claims
// 4. If all validators fail, returns appropriate error
//
// # Emulator Mode
//
// For local development, emulator mode allows the token "owner" to bypass validation
// and grant full admin access. This should never be enabled in production.
//
// # Claims
//
// Validated tokens produce Claims containing:
//   - Subject (uid): The authenticated user's unique identifier
//   - Provider: How the user authenticated (anonymous, google, custom, etc.)
//   - IsAdmin: Whether the user has admin privileges
//   - Custom: Additional claims from the token (e.g., roles, permissions)
//
// These claims are forwarded to the backend server where security rules can
// reference them (e.g., auth.uid, auth.token.admin).
//
// # Error Handling
//
// Auth errors are carefully mapped to user-friendly messages via UserFriendlyError().
// This provides enough information to debug configuration issues without leaking
// sensitive details about the token validation process.
package auth

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/lark-sh/lark/edge/logger"
)

// Standard errors for auth operations.
var (
	ErrNoToken       = errors.New("no token provided")
	ErrInvalidToken  = errors.New("invalid token")
	ErrExpiredToken  = errors.New("token expired")
	ErrInvalidClaims = errors.New("invalid claims")
)

// UserFriendlyError returns a user-friendly error message for auth errors.
// This provides enough detail to debug issues without revealing sensitive information.
func UserFriendlyError(err error) string {
	if err == nil {
		return ""
	}

	// Check for specific error types
	switch {
	case errors.Is(err, ErrNoToken):
		return "no token provided"
	case errors.Is(err, ErrExpiredToken):
		return "token expired - please reauthenticate"
	case errors.Is(err, ErrInvalidIssuer):
		return "invalid token issuer - check firebase_project_id configuration"
	case errors.Is(err, ErrInvalidAudience):
		return "invalid token audience - check firebase_project_id configuration"
	case errors.Is(err, ErrKeyNotFound):
		return "signing key not found - token may be using an old or invalid key"
	case errors.Is(err, ErrInvalidKeyID):
		return "missing or invalid key ID in token header"
	case errors.Is(err, ErrInvalidServiceAccount):
		return "service account not authorized for this project"
	case errors.Is(err, ErrInvalidClaims):
		// Extract more specific message if available
		errMsg := err.Error()
		if strings.Contains(errMsg, "missing uid") {
			return "token missing required 'uid' field"
		}
		if strings.Contains(errMsg, "missing sub") {
			return "token missing required 'sub' field"
		}
		return "invalid token claims"
	case errors.Is(err, ErrInvalidToken):
		// Try to extract more specific reason from wrapped error
		errMsg := err.Error()
		if strings.Contains(errMsg, "signature") {
			return "invalid token signature - check secret key configuration"
		}
		if strings.Contains(errMsg, "algorithm") || strings.Contains(errMsg, "signing method") {
			return "unsupported token algorithm"
		}
		if strings.Contains(errMsg, "malformed") {
			return "malformed token"
		}
		return "invalid token"
	default:
		// For unknown errors, return a generic message
		return "authentication failed"
	}
}

// Claims represents the JWT claims for Lark authentication.
// Standard claims (sub, exp, iat) are handled by jwt.RegisteredClaims.
// Custom claims are stored in the Custom map.
type Claims struct {
	jwt.RegisteredClaims
	Provider string         `json:"provider,omitempty"` // Auth provider: anonymous, google, custom, etc.
	Custom   map[string]any `json:"claims,omitempty"`   // Custom claims (becomes auth.token in rules)
	Server   string         `json:"server,omitempty"`   // Assigned server ID (from coordinator)
	Project  string         `json:"project,omitempty"`  // Project ID (for rules lookup)
}

// Info holds the extracted authentication information for use in rules.
type Info struct {
	UID         string         // User ID (from sub claim)
	Provider    string         // Auth provider
	Token       map[string]any // Custom claims
	DatabaseID  string         // Database ID (from aud claim), format: "project/database"
	ServerID    string         // Assigned server ID (from server claim)
	ProjectID   string         // Project ID (from project claim)
	IsTrueAdmin bool           // True if token was signed with admin_secret_key (kid: "coordinator")
}

// Validator validates JWT tokens.
type Validator struct {
	secret []byte
}

// NewValidator creates a new JWT validator with the given secret.
func NewValidator(secret []byte) *Validator {
	return &Validator{secret: secret}
}

// Validate validates a JWT token and returns the auth info.
// Returns an error if the token is invalid, expired, or malformed.
func (v *Validator) Validate(tokenString string) (*Info, error) {
	if tokenString == "" {
		return nil, ErrNoToken
	}

	// Parse and validate the token
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return v.secret, nil
	}, jwt.WithExpirationRequired())

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Extract UID from subject
	uid := claims.Subject
	if uid == "" {
		return nil, fmt.Errorf("%w: missing sub claim", ErrInvalidClaims)
	}

	// Extract database ID from audience (first audience if multiple)
	var databaseID string
	if len(claims.Audience) > 0 {
		databaseID = claims.Audience[0]
	}

	// Build auth info
	info := &Info{
		UID:        uid,
		Provider:   claims.Provider,
		Token:      claims.Custom,
		DatabaseID: databaseID,
		ServerID:   claims.Server,
		ProjectID:  claims.Project,
	}

	// Default provider to "custom" if not specified
	if info.Provider == "" {
		info.Provider = "custom"
	}

	// Ensure Token map is not nil
	if info.Token == nil {
		info.Token = make(map[string]any)
	}

	return info, nil
}

// GenerateToken creates a signed JWT token (useful for testing).
func GenerateToken(secret []byte, uid, provider string, customClaims map[string]any, expiry time.Duration) (string, error) {
	now := time.Now()

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uid,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(expiry)),
		},
		Provider: provider,
		Custom:   customClaims,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// GenerateTestToken is a convenience function for tests.
// Creates a token with 1 hour expiry.
func GenerateTestToken(secret []byte, uid string, customClaims map[string]any) (string, error) {
	return GenerateToken(secret, uid, "anonymous", customClaims, time.Hour)
}

// MultiValidator validates Lark project tokens (HS256) and Firebase ID tokens (RS256).
// Token validation is done via ValidateForProject() which uses project-specific keys.
type MultiValidator struct {
	firebaseValidator *FirebaseValidator
	emulatorMode      bool // If true, accepts "owner" as admin token (for local testing only)
}

// NewMultiValidator creates a validator that supports both Lark and Firebase tokens.
// firebaseProjectIDs are the allowed Firebase project IDs for RS256 tokens.
func NewMultiValidator(firebaseProjectIDs []string) *MultiValidator {
	return &MultiValidator{
		firebaseValidator: NewFirebaseValidator(firebaseProjectIDs),
		emulatorMode:      false, // Disabled by default for security
	}
}

// SetEmulatorMode enables or disables emulator mode.
// When enabled, the special "owner" token is accepted as an admin token.
// WARNING: Only enable this for local testing, never in production!
func (v *MultiValidator) SetEmulatorMode(enabled bool) {
	v.emulatorMode = enabled
}

// AddFirebaseProjectID adds a Firebase project ID to the allowed list.
func (v *MultiValidator) AddFirebaseProjectID(projectID string) {
	v.firebaseValidator.AddProjectID(projectID)
}

// ValidateForProject validates a token using project-specific keys.
// Supports 5 token types, auto-detected by algorithm and payload fields:
//
// RS256 tokens:
//   - iss starts with "https://securetoken.google.com/" → Firebase ID Token
//   - aud = identitytoolkit URL → Firebase Custom Token
//
// HS256 tokens:
//   - kid: "coordinator" → Coordinator Admin Token (use adminSecretKey, grants IsTrueAdmin)
//   - Has "d" field, no "uid" → Firebase Legacy Token (use secretKey)
//   - Has "uid" field → Lark Customer Token (use secretKey)
//
// Parameters:
//   - secretKey: Project's customer-facing secret (for customer-signed tokens)
//   - adminSecretKey: Coordinator's admin secret (for dashboard/admin tokens)
//   - firebaseProjectID: Firebase project ID (for RS256 token validation)
func (v *MultiValidator) ValidateForProject(tokenString string, secretKey string, adminSecretKey string, firebaseProjectID ...string) (*Info, error) {
	if tokenString == "" {
		return nil, ErrNoToken
	}

	// Handle special "owner" token for emulator/testing mode
	if tokenString == "owner" {
		if !v.emulatorMode {
			return nil, ErrInvalidToken
		}
		// Audit signal: every emulator owner-token acceptance gets logged so
		// that an accidentally-enabled emulator mode in prod is loud, not silent.
		logger.Warn("emulator owner-token accepted — should never appear in production")
		return &Info{
			UID:         "owner",
			Provider:    "owner",
			IsTrueAdmin: true, // Owner token grants true admin access
			Token: map[string]any{
				"isAdmin": true,
			},
		}, nil
	}

	// Peek at the token header to determine algorithm and kid
	alg, kid, err := peekTokenHeader(tokenString)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	// Get optional Firebase project ID
	var fbProjectID string
	if len(firebaseProjectID) > 0 {
		fbProjectID = firebaseProjectID[0]
	}

	switch alg {
	case "HS256":
		return v.validateHS256Token(tokenString, secretKey, adminSecretKey, kid)
	case "RS256":
		return v.validateRS256Token(tokenString, fbProjectID)
	default:
		return nil, fmt.Errorf("%w: unsupported algorithm %s", ErrInvalidToken, alg)
	}
}

// validateHS256Token validates an HS256 token using the appropriate key based on kid and payload.
// Detection logic:
//   - kid: "coordinator" → Coordinator Admin Token (use adminSecretKey)
//   - Has "d" field, no "uid" → Firebase Legacy Token (use secretKey)
//   - Has "uid" field → Lark Customer Token (use secretKey)
func (v *MultiValidator) validateHS256Token(tokenString, secretKey, adminSecretKey, kid string) (*Info, error) {
	if kid == "coordinator" {
		// Coordinator-signed admin token - must use adminSecretKey
		if adminSecretKey == "" {
			return nil, fmt.Errorf("%w: no admin secret key available for coordinator token", ErrInvalidToken)
		}
		validator := NewValidator([]byte(adminSecretKey))
		info, err := validator.Validate(tokenString)
		if err != nil {
			return nil, err
		}
		// Grant true admin access
		info.IsTrueAdmin = true
		return info, nil
	}

	// Customer-signed token - need secretKey
	if secretKey == "" {
		return nil, fmt.Errorf("%w: no secret key available", ErrInvalidToken)
	}

	// Peek at payload to determine token type (Legacy vs Customer)
	payload, err := peekTokenPayload(tokenString)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	// Check for Firebase Legacy Token (has "d" field, no "uid" field)
	_, hasD := payload["d"]
	_, hasUID := payload["uid"]
	if hasD && !hasUID {
		// Firebase Legacy Token
		info, err := validateLegacyToken(tokenString, []byte(secretKey))
		if err != nil {
			return nil, err
		}
		info.IsTrueAdmin = false
		return info, nil
	}

	// Check for Lark Customer Token (has "uid" field)
	if hasUID {
		info, err := validateLarkCustomerToken(tokenString, []byte(secretKey))
		if err != nil {
			return nil, err
		}
		info.IsTrueAdmin = false
		return info, nil
	}

	// Fallback: Try the old validator that uses "sub" field
	// This maintains backward compatibility with existing tokens
	validator := NewValidator([]byte(secretKey))
	info, err := validator.Validate(tokenString)
	if err != nil {
		return nil, err
	}
	// Customer-signed tokens never get to attest these fields. The full Claims
	// struct decodes "server" and "project" from the JWT payload, but those are
	// authoritative metadata set by the coordinator on its own tokens — letting
	// a customer-signed token populate them risks the backend trusting the
	// claim as authoritative.
	info.IsTrueAdmin = false
	info.ServerID = ""
	info.ProjectID = ""
	return info, nil
}

// validateRS256Token validates an RS256 token (Firebase ID Token or Firebase Custom Token).
// Detection logic:
//   - iss starts with "https://securetoken.google.com/" → Firebase ID Token
//   - aud = identitytoolkit URL → Firebase Custom Token
func (v *MultiValidator) validateRS256Token(tokenString string, firebaseProjectID string) (*Info, error) {
	// Fail closed: Firebase tokens (ID *or* custom) are only accepted when the
	// project has a Firebase project ID configured.
	// A project that hasn't opted into Firebase must reject all Firebase tokens.
	if firebaseProjectID == "" {
		return nil, fmt.Errorf("%w: Firebase tokens not accepted (no Firebase project ID configured)", ErrInvalidToken)
	}

	// Peek at payload to determine token type
	payload, err := peekTokenPayload(tokenString)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	// Check issuer for Firebase ID Token detection
	if iss, ok := payload["iss"].(string); ok {
		const idTokenIssuerPrefix = "https://securetoken.google.com/"
		if len(iss) > len(idTokenIssuerPrefix) && iss[:len(idTokenIssuerPrefix)] == idTokenIssuerPrefix {
			// Firebase ID Token - validate with Google's securetoken keys
			// Pass expected project ID for issuer/audience validation
			return v.firebaseValidator.ValidateForProjectID(tokenString, firebaseProjectID)
		}
	}

	// Check audience for Firebase Custom Token detection
	var aud string
	if audClaim, ok := payload["aud"]; ok {
		// Audience can be string or []string
		switch a := audClaim.(type) {
		case string:
			aud = a
		case []any:
			if len(a) > 0 {
				if s, ok := a[0].(string); ok {
					aud = s
				}
			}
		}
	}

	const customTokenAudience = "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit"
	if aud == customTokenAudience {
		// Firebase Custom Token - validate with service account keys
		if firebaseProjectID == "" {
			return nil, fmt.Errorf("%w: firebase project ID required for custom token validation", ErrInvalidToken)
		}
		return v.firebaseValidator.ValidateCustomToken(tokenString, firebaseProjectID)
	}

	// Unknown RS256 token type
	return nil, fmt.Errorf("%w: unrecognized RS256 token format", ErrInvalidToken)
}

// peekTokenHeader extracts the algorithm and kid from a JWT header without full parsing.
// Returns (alg, kid, error). kid may be empty if not present in header.
func peekTokenHeader(tokenString string) (string, string, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return "", "", err
	}

	alg, ok := token.Header["alg"].(string)
	if !ok {
		return "", "", errors.New("missing algorithm in token header")
	}

	// kid is optional
	kid, _ := token.Header["kid"].(string)

	return alg, kid, nil
}

// reservedClaims lists JWT and Firebase-reserved claim names that should NOT be
// included in custom claims extraction. These are standard/reserved fields.
var reservedClaims = map[string]bool{
	// Standard JWT claims (RFC 7519)
	"iss": true, "sub": true, "aud": true, "exp": true,
	"iat": true, "nbf": true, "jti": true,
	// Firebase-specific claims
	"auth_time": true, "firebase": true, "user_id": true,
	"email": true, "email_verified": true,
	"name": true, "picture": true, "phone_number": true,
}

// findRootClaims extracts custom claims from the root level of a JWT payload.
// Firebase ID Tokens have custom claims at the root level (not nested in a "claims" object).
// This function filters out reserved JWT and Firebase-specific fields, returning only
// the custom claims that were set via setCustomUserClaims() or through Custom Token claims.
//
// Example: If a token has {"sub": "user1", "iss": "...", "is_admin": true, "role": "gm"},
// this returns {"is_admin": true, "role": "gm"}.
func findRootClaims(payload map[string]any) map[string]any {
	claims := make(map[string]any)
	for key, value := range payload {
		if !reservedClaims[key] {
			claims[key] = value
		}
	}
	return claims
}

// peekTokenPayload extracts the payload from a JWT without full parsing/verification.
// Returns the payload as a map for inspection.
func peekTokenPayload(tokenString string) (jwt.MapClaims, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("failed to parse claims as map")
	}

	return claims, nil
}

// LegacyClaims represents the claims in a Firebase Legacy token.
// These are old Firebase custom tokens from before Firebase Auth existed.
type LegacyClaims struct {
	jwt.RegisteredClaims
	Version string         `json:"v,omitempty"`
	D       map[string]any `json:"d,omitempty"` // Custom claims in legacy format
}

// validateLegacyToken validates a Firebase Legacy Token (HS256 with d field).
// Returns auth info with empty UID (legacy tokens predate user ID concept).
//
// Unlike every other token type, legacy tokens do NOT require an `exp` claim:
// the Firebase 2.x FirebaseTokenGenerator made expiration optional (a
// "never-expire" token was valid), so enforcing it here would reject tokens
// that legacy clients legitimately mint. Expiration is still checked when present.
func validateLegacyToken(tokenString string, secretKey []byte) (*Info, error) {
	token, err := jwt.ParseWithClaims(tokenString, &LegacyClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secretKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*LegacyClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Legacy tokens have empty UID - they predate user ID concept
	// Applications should use claims in `d` for user identification if needed
	info := &Info{
		UID:      "",
		Provider: "custom",
		Token:    claims.D,
	}

	// Ensure Token map is not nil
	if info.Token == nil {
		info.Token = make(map[string]any)
	}

	return info, nil
}

// LarkCustomerClaims represents the claims in a Lark Customer Token.
// Format intentionally matches Firebase Custom Token for easy migration.
type LarkCustomerClaims struct {
	jwt.RegisteredClaims
	UID    string         `json:"uid"`              // User ID
	Claims map[string]any `json:"claims,omitempty"` // Custom claims
}

// validateLarkCustomerToken validates a Lark Customer Token (HS256 with uid field).
func validateLarkCustomerToken(tokenString string, secretKey []byte) (*Info, error) {
	token, err := jwt.ParseWithClaims(tokenString, &LarkCustomerClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secretKey, nil
	}, jwt.WithExpirationRequired())

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*LarkCustomerClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// UID is required for Lark Customer Tokens
	if claims.UID == "" {
		return nil, fmt.Errorf("%w: missing uid", ErrInvalidClaims)
	}

	info := &Info{
		UID:      claims.UID,
		Provider: "custom",
		Token:    claims.Claims,
	}

	// Ensure Token map is not nil
	if info.Token == nil {
		info.Token = make(map[string]any)
	}

	return info, nil
}
