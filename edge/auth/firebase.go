package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Google's public key endpoint for Firebase Auth (ID tokens)
const googleCertsURL = "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"

// Base URL for fetching service account public keys (Custom tokens)
const googleServiceAccountCertsBase = "https://www.googleapis.com/robot/v1/metadata/x509/"

// Expected audience for Firebase Custom Tokens
const firebaseCustomTokenAudience = "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit"

// Cache refresh interval for Google's public keys
const keyRefreshInterval = 1 * time.Hour

// Firebase-specific errors
var (
	ErrInvalidIssuer         = errors.New("invalid token issuer")
	ErrInvalidAudience       = errors.New("invalid token audience")
	ErrKeyNotFound           = errors.New("signing key not found")
	ErrInvalidKeyID          = errors.New("missing or invalid key ID")
	ErrInvalidServiceAccount = errors.New("invalid service account for project")
)

// FirebaseClaims represents the claims in a Firebase ID token.
type FirebaseClaims struct {
	jwt.RegisteredClaims
	AuthTime int64        `json:"auth_time"`         // Time of authentication
	Firebase FirebaseInfo `json:"firebase"`          // Firebase-specific info
	UID      string       `json:"user_id"`           // User ID (also in sub)
	Email    string       `json:"email,omitempty"`   // User's email
	Name     string       `json:"name,omitempty"`    // User's display name
	Picture  string       `json:"picture,omitempty"` // User's profile picture URL
}

// FirebaseInfo contains Firebase-specific authentication info.
type FirebaseInfo struct {
	SignInProvider string              `json:"sign_in_provider"` // e.g., "google.com", "password"
	Identities     map[string][]string `json:"identities"`       // Linked identities
	Tenant         string              `json:"tenant,omitempty"` // Multi-tenancy tenant ID
}

// FirebaseCustomTokenClaims represents the claims in a Firebase Custom Token.
// These are created by backend servers using Firebase Admin SDK.
type FirebaseCustomTokenClaims struct {
	jwt.RegisteredClaims
	UID    string         `json:"uid"`              // User ID (NOT sub - sub is the service account)
	Claims map[string]any `json:"claims,omitempty"` // Custom claims
}

// GoogleKeyCache caches Google's public keys for Firebase ID token verification.
type GoogleKeyCache struct {
	mu          sync.RWMutex
	keys        map[string]*rsa.PublicKey // kid -> public key
	lastRefresh time.Time
	httpClient  *http.Client
}

// NewGoogleKeyCache creates a new cache for Google's public keys.
func NewGoogleKeyCache() *GoogleKeyCache {
	return &GoogleKeyCache{
		keys: make(map[string]*rsa.PublicKey),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetKey returns the public key for the given key ID.
// Automatically refreshes the cache if needed.
func (c *GoogleKeyCache) GetKey(kid string) (*rsa.PublicKey, error) {
	c.mu.RLock()
	key, ok := c.keys[kid]
	needsRefresh := time.Since(c.lastRefresh) > keyRefreshInterval
	c.mu.RUnlock()

	if ok && !needsRefresh {
		return key, nil
	}

	// Refresh keys
	if err := c.refresh(); err != nil {
		// If refresh fails but we have a cached key, use it
		if ok {
			return key, nil
		}
		return nil, fmt.Errorf("failed to fetch Google keys: %w", err)
	}

	c.mu.RLock()
	key, ok = c.keys[kid]
	c.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, kid)
	}

	return key, nil
}

// refresh fetches the latest public keys from Google.
func (c *GoogleKeyCache) refresh() error {
	resp, err := c.httpClient.Get(googleCertsURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse the JSON response (map of kid -> PEM certificate)
	var certs map[string]string
	if err := json.Unmarshal(body, &certs); err != nil {
		return fmt.Errorf("failed to parse certificates: %w", err)
	}

	// Parse certificates into public keys
	newKeys := make(map[string]*rsa.PublicKey)
	for kid, certPEM := range certs {
		key, err := parsePublicKeyFromPEM(certPEM)
		if err != nil {
			// Log but don't fail - some keys might be in different formats
			continue
		}
		newKeys[kid] = key
	}

	if len(newKeys) == 0 {
		return errors.New("no valid keys found in response")
	}

	c.mu.Lock()
	c.keys = newKeys
	c.lastRefresh = time.Now()
	c.mu.Unlock()

	return nil
}

// parsePublicKeyFromPEM extracts an RSA public key from a PEM-encoded certificate.
func parsePublicKeyFromPEM(pemStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	rsaKey, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("certificate does not contain RSA public key")
	}

	return rsaKey, nil
}

// serviceAccountKeyEntry holds cached keys for a service account.
type serviceAccountKeyEntry struct {
	keys        map[string]*rsa.PublicKey
	lastRefresh time.Time
}

// ServiceAccountKeyCache caches public keys for Firebase service accounts.
// Keys are fetched per-service-account from Google's metadata endpoint.
type ServiceAccountKeyCache struct {
	mu         sync.RWMutex
	accounts   map[string]*serviceAccountKeyEntry // email -> keys
	httpClient *http.Client
}

// NewServiceAccountKeyCache creates a new cache for service account public keys.
func NewServiceAccountKeyCache() *ServiceAccountKeyCache {
	return &ServiceAccountKeyCache{
		accounts: make(map[string]*serviceAccountKeyEntry),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetKey returns the public key for the given service account and key ID.
// Automatically fetches and caches keys if needed.
func (c *ServiceAccountKeyCache) GetKey(serviceAccountEmail, kid string) (*rsa.PublicKey, error) {
	c.mu.RLock()
	entry, hasEntry := c.accounts[serviceAccountEmail]
	var key *rsa.PublicKey
	var hasKey bool
	var needsRefresh bool
	if hasEntry {
		key, hasKey = entry.keys[kid]
		needsRefresh = time.Since(entry.lastRefresh) > keyRefreshInterval
	}
	c.mu.RUnlock()

	if hasKey && !needsRefresh {
		return key, nil
	}

	// Fetch keys for this service account
	if err := c.refreshServiceAccount(serviceAccountEmail); err != nil {
		// If refresh fails but we have a cached key, use it
		if hasKey {
			return key, nil
		}
		return nil, fmt.Errorf("failed to fetch service account keys: %w", err)
	}

	c.mu.RLock()
	entry = c.accounts[serviceAccountEmail]
	if entry != nil {
		key, hasKey = entry.keys[kid]
	}
	c.mu.RUnlock()

	if !hasKey {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, kid)
	}

	return key, nil
}

// refreshServiceAccount fetches the latest public keys for a service account.
func (c *ServiceAccountKeyCache) refreshServiceAccount(email string) error {
	url := googleServiceAccountCertsBase + email
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse the JSON response (map of kid -> PEM certificate)
	var certs map[string]string
	if err := json.Unmarshal(body, &certs); err != nil {
		return fmt.Errorf("failed to parse certificates: %w", err)
	}

	// Parse certificates into public keys
	newKeys := make(map[string]*rsa.PublicKey)
	for kid, certPEM := range certs {
		key, err := parsePublicKeyFromPEM(certPEM)
		if err != nil {
			// Log but don't fail - some keys might be in different formats
			continue
		}
		newKeys[kid] = key
	}

	if len(newKeys) == 0 {
		return errors.New("no valid keys found in response")
	}

	c.mu.Lock()
	c.accounts[email] = &serviceAccountKeyEntry{
		keys:        newKeys,
		lastRefresh: time.Now(),
	}
	c.mu.Unlock()

	return nil
}

// FirebaseValidator validates Firebase ID tokens and Firebase Custom tokens.
type FirebaseValidator struct {
	idTokenKeyCache     *GoogleKeyCache         // For Firebase ID Tokens
	customTokenKeyCache *ServiceAccountKeyCache // For Firebase Custom Tokens
	allowedProjectIDs   map[string]bool         // Allowed Firebase project IDs
}

// NewFirebaseValidator creates a new Firebase token validator.
// projectIDs are the allowed Firebase project IDs (used to validate issuer and audience).
func NewFirebaseValidator(projectIDs []string) *FirebaseValidator {
	allowed := make(map[string]bool, len(projectIDs))
	for _, id := range projectIDs {
		allowed[id] = true
	}
	return &FirebaseValidator{
		idTokenKeyCache:     NewGoogleKeyCache(),
		customTokenKeyCache: NewServiceAccountKeyCache(),
		allowedProjectIDs:   allowed,
	}
}

// AddProjectID adds a Firebase project ID to the allowed list.
func (v *FirebaseValidator) AddProjectID(projectID string) {
	v.allowedProjectIDs[projectID] = true
}

// ValidateForProjectID validates a Firebase ID token against a specific expected project ID.
// This is used when we know the expected project (e.g., from Lark project config).
func (v *FirebaseValidator) ValidateForProjectID(tokenString string, expectedProjectID string) (*Info, error) {
	if tokenString == "" {
		return nil, ErrNoToken
	}

	// Fail closed: an ID token must always be pinned to a known project. An empty
	// expected project ID means the project hasn't enabled Firebase auth tokens.
	if expectedProjectID == "" {
		return nil, fmt.Errorf("%w: no expected Firebase project configured", ErrInvalidIssuer)
	}

	// Parse without verification first to get the key ID and raw claims
	token, _, err := jwt.NewParser().ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	rawClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Get the key ID from the header
	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return nil, ErrInvalidKeyID
	}

	// Get the public key from Google's cache
	publicKey, err := v.idTokenKeyCache.GetKey(kid)
	if err != nil {
		return nil, err
	}

	// Parse and validate with the key
	token, err = jwt.ParseWithClaims(tokenString, &FirebaseClaims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return publicKey, nil
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

	claims, ok := token.Claims.(*FirebaseClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Validate issuer format and extract project ID
	const issuerPrefix = "https://securetoken.google.com/"
	issuer := claims.Issuer
	if len(issuer) <= len(issuerPrefix) || issuer[:len(issuerPrefix)] != issuerPrefix {
		return nil, fmt.Errorf("%w: %s", ErrInvalidIssuer, issuer)
	}
	projectIDFromIssuer := issuer[len(issuerPrefix):]

	// Validate issuer matches the EXPECTED project ID (not a global list).
	// expectedProjectID is guaranteed non-empty by the guard above, so this is
	// an unconditional pin — the token's project must equal the configured one.
	if projectIDFromIssuer != expectedProjectID {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrInvalidIssuer, expectedProjectID, projectIDFromIssuer)
	}

	// Validate audience matches issuer's project ID
	if len(claims.Audience) == 0 || claims.Audience[0] != projectIDFromIssuer {
		return nil, fmt.Errorf("%w: expected %s", ErrInvalidAudience, projectIDFromIssuer)
	}

	// Extract UID
	uid := claims.Subject
	if uid == "" {
		uid = claims.UID
	}
	if uid == "" {
		return nil, fmt.Errorf("%w: missing uid", ErrInvalidClaims)
	}

	// Build auth info
	info := &Info{
		UID:       uid,
		Provider:  claims.Firebase.SignInProvider,
		ProjectID: projectIDFromIssuer,
		Token:     findRootClaims(rawClaims),
	}

	if claims.Firebase.SignInProvider != "" {
		info.Token["sign_in_provider"] = claims.Firebase.SignInProvider
	}
	if len(claims.Firebase.Identities) > 0 {
		info.Token["identities"] = claims.Firebase.Identities
	}
	if claims.Firebase.Tenant != "" {
		info.Token["tenant"] = claims.Firebase.Tenant
	}
	if claims.Email != "" {
		info.Token["email"] = claims.Email
	}
	if claims.Name != "" {
		info.Token["name"] = claims.Name
	}
	if claims.Picture != "" {
		info.Token["picture"] = claims.Picture
	}

	return info, nil
}

// Validate validates a Firebase ID token and returns auth info.
// For Firebase ID Tokens, custom claims appear at the ROOT level of the JWT payload
// (not nested in a "claims" object). This function extracts them using findRootClaims().
func (v *FirebaseValidator) Validate(tokenString string) (*Info, error) {
	if tokenString == "" {
		return nil, ErrNoToken
	}

	// Parse without verification first to get the key ID and raw claims
	// We use MapClaims to get all fields including custom claims at root level
	token, _, err := jwt.NewParser().ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	rawClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Get the key ID from the header
	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return nil, ErrInvalidKeyID
	}

	// Get the public key
	publicKey, err := v.idTokenKeyCache.GetKey(kid)
	if err != nil {
		return nil, err
	}

	// Now parse and validate with the key using FirebaseClaims for structured access
	token, err = jwt.ParseWithClaims(tokenString, &FirebaseClaims{}, func(t *jwt.Token) (any, error) {
		// Validate signing method
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return publicKey, nil
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

	claims, ok := token.Claims.(*FirebaseClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Validate issuer format: https://securetoken.google.com/{projectId}
	// The prefix is 32 characters: "https://securetoken.google.com/"
	const issuerPrefix = "https://securetoken.google.com/"
	issuer := claims.Issuer
	if len(issuer) <= len(issuerPrefix) || issuer[:len(issuerPrefix)] != issuerPrefix {
		return nil, fmt.Errorf("%w: %s", ErrInvalidIssuer, issuer)
	}
	projectIDFromIssuer := issuer[len(issuerPrefix):] // Extract project ID from issuer

	// Validate issuer matches a known project
	if !v.allowedProjectIDs[projectIDFromIssuer] {
		return nil, fmt.Errorf("%w: unknown project %s", ErrInvalidIssuer, projectIDFromIssuer)
	}

	// Validate audience matches issuer's project ID
	if len(claims.Audience) == 0 || claims.Audience[0] != projectIDFromIssuer {
		return nil, fmt.Errorf("%w: expected %s", ErrInvalidAudience, projectIDFromIssuer)
	}

	// Extract UID (prefer sub, fall back to user_id)
	uid := claims.Subject
	if uid == "" {
		uid = claims.UID
	}
	if uid == "" {
		return nil, fmt.Errorf("%w: missing uid", ErrInvalidClaims)
	}

	// Build auth info with custom claims extracted from root level
	// Custom claims in Firebase ID Tokens are at the root, not in a nested "claims" object
	info := &Info{
		UID:       uid,
		Provider:  claims.Firebase.SignInProvider,
		ProjectID: projectIDFromIssuer,
		Token:     findRootClaims(rawClaims),
	}

	// Add Firebase-specific info to token map (these are useful in rules)
	if claims.Firebase.SignInProvider != "" {
		info.Token["sign_in_provider"] = claims.Firebase.SignInProvider
	}
	if len(claims.Firebase.Identities) > 0 {
		info.Token["identities"] = claims.Firebase.Identities
	}

	// Default provider if not set
	if info.Provider == "" {
		info.Provider = "firebase"
	}

	return info, nil
}

// ValidateCustomToken validates a Firebase Custom Token (RS256).
// These are created by backend servers using Firebase Admin SDK and can be sent
// directly to Lark or exchanged for an ID token via Firebase Auth.
//
// firebaseProjectID is the expected Firebase project ID. The service account
// issuing the token must belong to this project (email must end with @{projectID}.iam.gserviceaccount.com).
func (v *FirebaseValidator) ValidateCustomToken(tokenString string, firebaseProjectID string) (*Info, error) {
	if tokenString == "" {
		return nil, ErrNoToken
	}

	// Fail closed: a custom token must be pinned to a known project. With an empty
	// project ID the service-account suffix check below degenerates to
	// "@.iam.gserviceaccount.com" (which no real account matches, so it's safe in
	// practice) — but we reject explicitly so the guarantee doesn't depend on that
	// accident and can't be weakened by a future refactor.
	if firebaseProjectID == "" {
		return nil, fmt.Errorf("%w: no expected Firebase project configured", ErrInvalidServiceAccount)
	}

	// Parse without verification first to get the header and claims
	token, _, err := jwt.NewParser().ParseUnverified(tokenString, &FirebaseCustomTokenClaims{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	// Get the key ID from the header
	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return nil, ErrInvalidKeyID
	}

	// Get claims to extract issuer (service account email)
	claims, ok := token.Claims.(*FirebaseCustomTokenClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Validate issuer is a service account from the expected project
	// Format: {name}@{projectID}.iam.gserviceaccount.com
	serviceAccountEmail := claims.Issuer
	expectedSuffix := "@" + firebaseProjectID + ".iam.gserviceaccount.com"
	if !strings.HasSuffix(serviceAccountEmail, expectedSuffix) {
		return nil, fmt.Errorf("%w: expected service account from project %s, got %s",
			ErrInvalidServiceAccount, firebaseProjectID, serviceAccountEmail)
	}

	// Validate audience is the Firebase Identity Toolkit URL
	if len(claims.Audience) == 0 || claims.Audience[0] != firebaseCustomTokenAudience {
		return nil, fmt.Errorf("%w: expected %s", ErrInvalidAudience, firebaseCustomTokenAudience)
	}

	// Get the public key for this service account
	publicKey, err := v.customTokenKeyCache.GetKey(serviceAccountEmail, kid)
	if err != nil {
		return nil, err
	}

	// Now parse and validate with the key
	token, err = jwt.ParseWithClaims(tokenString, &FirebaseCustomTokenClaims{}, func(t *jwt.Token) (any, error) {
		// Validate signing method
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return publicKey, nil
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

	claims, ok = token.Claims.(*FirebaseCustomTokenClaims)
	if !ok {
		return nil, ErrInvalidClaims
	}

	// Extract UID - for custom tokens, use the uid field (NOT sub, which is the service account)
	uid := claims.UID
	if uid == "" {
		return nil, fmt.Errorf("%w: missing uid", ErrInvalidClaims)
	}

	// Build auth info
	info := &Info{
		UID:       uid,
		Provider:  "custom",
		ProjectID: firebaseProjectID,
		Token:     claims.Claims,
	}

	// Ensure Token map is not nil
	if info.Token == nil {
		info.Token = make(map[string]any)
	}

	return info, nil
}
