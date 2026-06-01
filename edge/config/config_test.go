package config

import (
	"os"
	"testing"
)

func clearEnv() {
	// Clear all config-related env vars
	envVars := []string{
		"DATABASE_URL", "SERVER_SECRET",
		"HTTPS_LISTEN_ADDR", "WT_LISTEN_ADDR", "WT_PORTS",
		"TLS_CERT_FILE", "TLS_KEY_FILE",
		"CERTMAGIC_ENABLED", "CERTMAGIC_EMAIL", "CERTMAGIC_DOMAINS", "CERTMAGIC_STORAGE",
		"CLOUDFLARE_API_TOKEN",
		"BACKEND_ADDRS", "COORDINATOR_ADDR", "IS_COORDINATOR",
		"LARKDB_DOMAIN",
		"HEARTBEAT_TIMEOUT", "DEATH_TIMEOUT",
		"BATCH_FLUSH_INTERVAL", "BATCH_MAX_SIZE", "BATCH_MAX_MESSAGES",
		"DEBUG",
	}
	for _, key := range envVars {
		os.Unsetenv(key)
	}
}

// testServerSecret is a ≥32-byte value so Load() passes the SERVER_SECRET
// strength guard (audit H-1) outside LOCAL_MODE.
const testServerSecret = "test-server-secret-0123456789abcdef"

func setRequiredEnvVars() {
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost/testdb")
	os.Setenv("SERVER_SECRET", testServerSecret)
}

func TestLoadWithRequiredVars(t *testing.T) {
	clearEnv()
	setRequiredEnvVars()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check required vars are set
	if cfg.DatabaseURL != "postgres://test:test@localhost/testdb" {
		t.Errorf("DatabaseURL: got %q", cfg.DatabaseURL)
	}
	if cfg.ServerSecret != testServerSecret {
		t.Errorf("ServerSecret: got %q", cfg.ServerSecret)
	}
}

func TestLoadMissingDatabaseURL(t *testing.T) {
	clearEnv()
	os.Setenv("SERVER_SECRET", "secret")

	_, err := Load()
	if err == nil {
		t.Error("expected error for missing DATABASE_URL")
	}
}

func TestLoadMissingServerSecret(t *testing.T) {
	clearEnv()
	os.Setenv("DATABASE_URL", "postgres://localhost/test")

	_, err := Load()
	if err == nil {
		t.Error("expected error for missing SERVER_SECRET")
	}
}

func TestDefaultValues(t *testing.T) {
	clearEnv()
	setRequiredEnvVars()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check defaults
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"HTTPSListenAddr", cfg.HTTPSListenAddr, ":443"},
		{"WebTransportListenAddr", cfg.WebTransportListenAddr, ":8444"},
		{"WebTransportPorts", cfg.WebTransportPorts, 1},
		{"TLSCertFile", cfg.TLSCertFile, "server.crt"},
		{"TLSKeyFile", cfg.TLSKeyFile, "server.key"},
		{"CertMagicEnabled", cfg.CertMagicEnabled, false},
		{"CertMagicStoragePath", cfg.CertMagicStoragePath, "./certs"},
		{"LarkDBDomain", cfg.LarkDBDomain, "larkdb.net"},
		{"HeartbeatTimeout", cfg.HeartbeatTimeout, 30},
		{"DeathTimeout", cfg.DeathTimeout, 60},
		{"BatchFlushInterval", cfg.BatchFlushInterval, 1},
		{"BatchMaxSize", cfg.BatchMaxSize, 65536},
		{"BatchMaxMessages", cfg.BatchMaxMessages, 100},
		{"IsSelfCoordinator", cfg.IsSelfCoordinator, true},
		{"Debug", cfg.Debug, false},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, tt.got, tt.want)
		}
	}
}

func TestCustomValues(t *testing.T) {
	clearEnv()
	setRequiredEnvVars()

	// Set custom values
	os.Setenv("HTTPS_LISTEN_ADDR", ":8080")
	os.Setenv("WT_LISTEN_ADDR", ":9001")
	os.Setenv("WT_PORTS", "4")
	os.Setenv("HEARTBEAT_TIMEOUT", "60")
	os.Setenv("DEATH_TIMEOUT", "120")
	os.Setenv("DEBUG", "true")
	os.Setenv("LARKDB_DOMAIN", "custom.net")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"HTTPSListenAddr", cfg.HTTPSListenAddr, ":8080"},
		{"WebTransportListenAddr", cfg.WebTransportListenAddr, ":9001"},
		{"WebTransportPorts", cfg.WebTransportPorts, 4},
		{"HeartbeatTimeout", cfg.HeartbeatTimeout, 60},
		{"DeathTimeout", cfg.DeathTimeout, 120},
		{"Debug", cfg.Debug, true},
		{"LarkDBDomain", cfg.LarkDBDomain, "custom.net"},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, tt.got, tt.want)
		}
	}
}

func TestBooleanParsing(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"1", true},
		{"yes", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"", false}, // defaults to false
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			clearEnv()
			setRequiredEnvVars()
			if tt.value != "" {
				os.Setenv("DEBUG", tt.value)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() failed: %v", err)
			}

			if cfg.Debug != tt.want {
				t.Errorf("DEBUG=%q: got %v, want %v", tt.value, cfg.Debug, tt.want)
			}
		})
	}
}

func TestIntegerParsing(t *testing.T) {
	tests := []struct {
		value    string
		want     int
		default_ int
	}{
		{"100", 100, 30},
		{"0", 0, 30},
		{"-1", -1, 30},
		{"invalid", 30, 30}, // falls back to default
		{"", 30, 30},        // falls back to default
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			clearEnv()
			setRequiredEnvVars()
			if tt.value != "" {
				os.Setenv("HEARTBEAT_TIMEOUT", tt.value)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() failed: %v", err)
			}

			if cfg.HeartbeatTimeout != tt.want {
				t.Errorf("HEARTBEAT_TIMEOUT=%q: got %d, want %d", tt.value, cfg.HeartbeatTimeout, tt.want)
			}
		})
	}
}

func TestCertMagicDomains(t *testing.T) {
	clearEnv()
	setRequiredEnvVars()
	os.Setenv("CERTMAGIC_ENABLED", "true")
	os.Setenv("CERTMAGIC_DOMAINS", "*.example.com,example.com,*.other.net")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if len(cfg.CertMagicDomains) != 3 {
		t.Errorf("CertMagicDomains: got %d domains, want 3", len(cfg.CertMagicDomains))
	}

	want := []string{"*.example.com", "example.com", "*.other.net"}
	for i, d := range want {
		if i >= len(cfg.CertMagicDomains) || cfg.CertMagicDomains[i] != d {
			t.Errorf("CertMagicDomains[%d]: got %v, want %v", i, cfg.CertMagicDomains, want)
			break
		}
	}
}

func TestCertMagicDomainsDefault(t *testing.T) {
	clearEnv()
	setRequiredEnvVars()
	os.Setenv("CERTMAGIC_ENABLED", "true")
	os.Setenv("LARKDB_DOMAIN", "test.net")
	// Don't set CERTMAGIC_DOMAINS

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Should default to wildcard + apex for LarkDB domain
	if len(cfg.CertMagicDomains) != 2 {
		t.Errorf("CertMagicDomains: got %d domains, want 2", len(cfg.CertMagicDomains))
	}

	want := []string{"*.test.net", "test.net"}
	for i, d := range want {
		if i >= len(cfg.CertMagicDomains) || cfg.CertMagicDomains[i] != d {
			t.Errorf("CertMagicDomains: got %v, want %v", cfg.CertMagicDomains, want)
			break
		}
	}
}

func TestBackendAddrs(t *testing.T) {
	clearEnv()
	setRequiredEnvVars()
	os.Setenv("BACKEND_ADDRS", "server1:8080,server2:8080,server3:8080")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if len(cfg.BackendAddrs) != 3 {
		t.Errorf("BackendAddrs: got %d, want 3", len(cfg.BackendAddrs))
	}

	want := []string{"server1:8080", "server2:8080", "server3:8080"}
	for i, addr := range want {
		if i >= len(cfg.BackendAddrs) || cfg.BackendAddrs[i] != addr {
			t.Errorf("BackendAddrs: got %v, want %v", cfg.BackendAddrs, want)
			break
		}
	}
}

func TestValidateServerSecret(t *testing.T) {
	strong := "0123456789abcdef0123456789abcdef" // 32 bytes

	tests := []struct {
		name      string
		secret    string
		localMode bool
		wantErr   bool
	}{
		{"empty rejected", "", false, true},
		{"known default rejected", defaultServerSecret, false, true},
		{"too short rejected", "short", false, true},
		{"31 bytes rejected", strong[:31], false, true},
		{"32 bytes accepted", strong, false, false},
		{"long random accepted", strong + strong, false, false},
		{"empty allowed in local mode", "", true, false},
		{"default allowed in local mode", defaultServerSecret, true, false},
		{"short allowed in local mode", "short", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServerSecret(tt.secret, tt.localMode)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateServerSecret(%q, local=%v): err=%v, wantErr=%v",
					tt.secret, tt.localMode, err, tt.wantErr)
			}
		})
	}
}

func TestIsLoopbackListenAddr(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8080", true},
		{"127.0.0.53:8080", true}, // 127.0.0.0/8 is all loopback
		{"[::1]:8080", true},
		{"localhost:8080", true},
		{":8080", false},        // empty host = all interfaces
		{"0.0.0.0:8080", false}, // explicit all-interfaces
		{"[::]:8080", false},    // all interfaces, IPv6
		{"192.168.1.10:8080", false},
		{"db.example.com:443", false}, // non-localhost hostname
		{"garbage", false},            // unparseable
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			if got := isLoopbackListenAddr(tt.addr); got != tt.want {
				t.Errorf("isLoopbackListenAddr(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}
