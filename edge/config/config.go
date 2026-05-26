// Package config handles environment-based configuration for the Lark proxy.
//
// # Overview
//
// All configuration is loaded from environment variables. This package provides
// a single Config struct that holds all settings, with sensible defaults where
// appropriate.
//
// # Key Configuration Areas
//
// Database:
//   - DATABASE_URL: Postgres connection string (required)
//
// TLS/Certificates:
//   - TLS_CERT_FILE, TLS_KEY_FILE: Static certificate files (for development)
//   - CERTMAGIC_ENABLED: Use Let's Encrypt for automatic certificates
//   - CERTMAGIC_DOMAINS: Domains to get certificates for (e.g., "*.larkdb.net")
//   - CLOUDFLARE_API_TOKEN: For DNS-01 challenge (required with CertMagic)
//
// Authentication:
//   - SERVER_SECRET: Shared secret for server-to-server auth
//
// Network:
//   - HTTPS_LISTEN_ADDR: Main port for WebSocket/REST (default: :443)
//   - WEBTRANSPORT_LISTEN_ADDR: Base UDP port for QUIC (default: :8444)
//   - WEBTRANSPORT_PORTS: Number of parallel UDP listeners (default: 1)
//
// Domains:
//   - LARKDB_DOMAIN: Client-facing domain (e.g., "larkdb.net")
//
// Timeouts:
//   - HEARTBEAT_TIMEOUT: Seconds before server is unhealthy (default: 30)
//   - DEATH_TIMEOUT: Seconds before server is marked offline (default: 60)
//
// Performance:
//   - BATCH_FLUSH_INTERVAL: Milliseconds between batch flushes (default: 1)
//   - BATCH_MAX_SIZE: Bytes before forced flush (default: 65536)
//
// # Local Mode
//
// When LOCAL_MODE=true, the proxy runs without TLS (plain HTTP) and uses an
// in-memory database instead of Postgres. This is useful for development.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Database
	DatabaseURL string

	// Server addresses
	HTTPSListenAddr        string // Main HTTPS port for WebSocket, REST API, and admin (default: 443)
	InternalListenAddr     string // Internal HTTP port for server-to-server traffic (default: :8080, empty to disable)
	WebTransportListenAddr string // Base port for WebTransport/QUIC (UDP, default: 8444)
	WebTransportPorts      int    // Number of parallel WebTransport listeners (default: 1)

	// TLS - Static certs (fallback if CertMagic not configured)
	TLSCertFile string
	TLSKeyFile  string

	// CertMagic - Automatic wildcard certs via Let's Encrypt
	CertMagicEnabled     bool     // Use CertMagic for automatic certs
	CertMagicEmail       string   // Email for Let's Encrypt account
	CertMagicDomains     []string // Domains to get certs for (e.g., "*.larkdb.net", "larkdb.net")
	CertMagicStoragePath string   // Where to store certs (default: ./certs)
	CertMagicStaging     bool     // Use Let's Encrypt staging environment (for testing)
	CertMagicResolvers   []string // Custom DNS resolvers for ACME validation (e.g., "8.8.8.8:53")
	CloudflareAPIToken   string   // Cloudflare API token for DNS-01 challenge

	// Authentication
	ServerSecret string // Shared secret for server-to-server auth

	// Backend configuration
	BackendAddrs []string // List of backend server addresses for direct connection

	// Coordinator settings
	CoordinatorAddr string // Address of coordinator API (for routing lookups if not self)
	IsSelfCoordinator bool  // If true, this proxy handles coordinator duties

	// Domain settings
	LarkDBDomain string // e.g., "larkdb.net" for *.larkdb.net routing

	// Timeouts (in seconds)
	HeartbeatTimeout int // How long before a server is considered unhealthy (default: 30)
	DeathTimeout     int // How long before a server is marked offline (default: 60)

	// Performance
	BatchFlushInterval int // Milliseconds between batch flushes (default: 1)
	BatchMaxSize       int // Max bytes before forced flush (default: 65536)
	BatchMaxMessages   int // Max messages before forced flush (default: 100)

	// Metrics aggregation
	MetricsFlushInterval time.Duration // How often the aggregator flushes database_metrics (default: 3m)

	// Backend connection settings
	ConnectionsPerCore int // Number of connections per server core (default: 2)

	// Admin API and dashboard
	AdminAPIEnabled bool // Enables /admin/* endpoints + embedded dashboard SPA

	// DisableTLS skips TLS termination on the main listener, so it serves
	// plain HTTP (and WebSocket as ws://). Required for local dev where
	// browsers exempt localhost from mixed-content rules. WebTransport is
	// disabled when this is on because QUIC requires TLS. LOCAL_MODE
	// implies DISABLE_TLS=true.
	DisableTLS bool

	// Debug
	Debug bool

	// Local development mode (bypasses database)
	LocalMode        bool   // If true, skip database and use local backend
	LocalBackendAddr string // Address of local backend (e.g., "localhost:7779")
	LocalProjectID   string // Project ID to use in local mode (default: "test-project")
}

func Load() (*Config, error) {
	cfg := &Config{
		// Defaults
		HTTPSListenAddr:        getEnv("HTTPS_LISTEN_ADDR", ":443"),
		InternalListenAddr:     getEnv("INTERNAL_LISTEN_ADDR", ":8080"),
		WebTransportListenAddr: getEnv("WT_LISTEN_ADDR", ":8444"),
		WebTransportPorts:      getEnvInt("WT_PORTS", 1),

		TLSCertFile: getEnv("TLS_CERT_FILE", "server.crt"),
		TLSKeyFile:  getEnv("TLS_KEY_FILE", "server.key"),

		// CertMagic
		CertMagicEnabled:     getEnvBool("CERTMAGIC_ENABLED", false),
		CertMagicEmail:       getEnv("CERTMAGIC_EMAIL", ""),
		CertMagicStoragePath: getEnv("CERTMAGIC_STORAGE", "./certs"),
		CertMagicStaging:     getEnvBool("CERTMAGIC_STAGING", false),

		LarkDBDomain: getEnv("LARKDB_DOMAIN", "larkdb.net"),

		HeartbeatTimeout: getEnvInt("HEARTBEAT_TIMEOUT", 30),
		DeathTimeout:     getEnvInt("DEATH_TIMEOUT", 60),

		BatchFlushInterval: getEnvInt("BATCH_FLUSH_INTERVAL", 1),
		BatchMaxSize:       getEnvInt("BATCH_MAX_SIZE", 65536),
		BatchMaxMessages:   getEnvInt("BATCH_MAX_MESSAGES", 100),

		MetricsFlushInterval: getEnvDuration("METRICS_FLUSH_INTERVAL", 3*time.Minute),

		ConnectionsPerCore: getEnvInt("CONNECTIONS_PER_CORE", 2),

		IsSelfCoordinator: getEnvBool("IS_COORDINATOR", true),
		AdminAPIEnabled:   getEnvBool("ADMIN_API_ENABLED", false),
		DisableTLS:        getEnvBool("DISABLE_TLS", false),
		Debug:             getEnvBool("DEBUG", false),

		// Local development mode
		LocalMode:        getEnvBool("LOCAL_MODE", false),
		LocalBackendAddr: getEnv("LOCAL_BACKEND_ADDR", "localhost:7779"),
		LocalProjectID:   getEnv("LOCAL_PROJECT_ID", "test-project"),
	}

	// Required environment variables (DATABASE_URL optional in local mode)
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" && !cfg.LocalMode {
		return nil, fmt.Errorf("DATABASE_URL is required (or set LOCAL_MODE=true)")
	}

	cfg.ServerSecret = os.Getenv("SERVER_SECRET")
	if cfg.ServerSecret == "" {
		return nil, fmt.Errorf("SERVER_SECRET is required")
	}

	// LOCAL_MODE implies DISABLE_TLS; nothing in the LOCAL_MODE deploy
	// expects to terminate TLS.
	if cfg.LocalMode {
		cfg.DisableTLS = true
	}

	// CertMagic domains, resolvers, and Cloudflare token
	if domains := os.Getenv("CERTMAGIC_DOMAINS"); domains != "" {
		cfg.CertMagicDomains = strings.Split(domains, ",")
	} else if cfg.CertMagicEnabled {
		// Default to wildcard + apex for LarkDB domain
		cfg.CertMagicDomains = []string{"*." + cfg.LarkDBDomain, cfg.LarkDBDomain}
	}
	if resolvers := os.Getenv("CERTMAGIC_RESOLVERS"); resolvers != "" {
		cfg.CertMagicResolvers = strings.Split(resolvers, ",")
	}
	cfg.CloudflareAPIToken = os.Getenv("CLOUDFLARE_API_TOKEN")

	// Backend addresses (comma-separated)
	if backends := os.Getenv("BACKEND_ADDRS"); backends != "" {
		cfg.BackendAddrs = strings.Split(backends, ",")
	}

	// Coordinator address (if not self)
	cfg.CoordinatorAddr = getEnv("COORDINATOR_ADDR", "")

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

// getEnvDuration parses a Go duration string (e.g. "3m", "90s"). Falls back to
// defaultVal if unset or unparseable.
func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		return val == "1" || val == "true" || val == "yes"
	}
	return defaultVal
}
