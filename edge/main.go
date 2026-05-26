package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lark-sh/lark/edge/api"
	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/cron"
	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
	"github.com/lark-sh/lark/edge/metrics"
	"github.com/lark-sh/lark/edge/notify"
	"github.com/lark-sh/lark/edge/proxy"
)

func main() {
	logger.Info("Starting lark-edge (thick proxy mode)")
	logger.Info("Runtime config", "GOMAXPROCS", runtime.GOMAXPROCS(0), "NumCPU", runtime.NumCPU())

	// Start pprof server for profiling
	go func() {
		logger.Debug("pprof server listening", "addr", ":6060")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			logger.Error("pprof server error", "error", err)
		}
	}()

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	if cfg.Debug {
		logger.Info("Debug mode enabled")
		logger.SetLevel(logger.LevelDebug)
	}

	// Set server name for structured logs (use hostname or env var)
	if hostname, err := os.Hostname(); err == nil {
		logger.SetServerName(hostname)
	}

	// Create backend connection pool with per-core sharding
	pool := backend.NewPool(cfg.ConnectionsPerCore)
	defer pool.Close()

	// Database and mode-specific setup
	var database db.Store
	var apiServer *api.Server
	var cleanup *cron.Cleanup
	var metricsAggregator *metrics.MetricsAggregator
	var configAdapter *configProviderAdapter

	if cfg.LocalMode {
		logger.Info("Local mode enabled", "backend", cfg.LocalBackendAddr, "project_id", cfg.LocalProjectID, "auth", "emulator")

		// Use mock database
		database = db.NewLocalDB(cfg.LocalProjectID, cfg.LocalBackendAddr)

		// Start background goroutine to connect to local backend
		// (matches production pattern where proxy starts first, then servers connect)
		go func() {
			addr := cfg.LocalBackendAddr
			for {
				if pool.HasBackend("local-server") {
					// Already connected
					time.Sleep(5 * time.Second)
					continue
				}

				logger.Debug("Attempting to connect to local backend", "address", addr)
				if err := pool.AddStaticBackend("local-server", addr); err != nil {
					logger.Debug("Local backend not ready, retrying", "error", err)
					time.Sleep(2 * time.Second)
					continue
				}
				logger.Info("Connected to local backend", "address", addr)
			}
		}()

		// No API server or cleanup in local mode
	} else {
		// Connect to real database (Postgres or SQLite, picked by DATABASE_URL scheme)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		database, err = db.New(ctx, cfg.DatabaseURL)
		cancel()
		if err != nil {
			logger.Error("Failed to connect to database", "error", err)
			os.Exit(1)
		}
		logger.Info("Connected to database", "driver", database.DriverKind())

		// Set up config provider for heartbeat writes and config fetches
		configAdapter = &configProviderAdapter{db: database}
		pool.SetConfigProvider(configAdapter)

		// Start server discovery loop (discovers servers from DB and connects to them)
		discoveryAdapter := &serverDiscoveryAdapter{db: database}
		pool.StartDiscoveryLoop(discoveryAdapter, 15*time.Second)

		// Create API server
		apiServer = api.New(cfg, database)
		apiServer.SetPool(pool)

		// Create metrics aggregator (flush interval env-configurable via METRICS_FLUSH_INTERVAL, default 3m)
		metricsAggregator = metrics.NewMetricsAggregator(database, cfg.MetricsFlushInterval)
		apiServer.SetMetricsAggregator(metricsAggregator)
		apiServer.RegisterMetricsRoutes()

		// Create cleanup job
		cleanup = cron.NewCleanup(database, cfg.DeathTimeout)

		// First-boot bootstrap: if the admin API is enabled and the
		// accounts table is empty, mint an initial admin account and
		// print the temporary password.
		if cfg.AdminAPIEnabled {
			bootCtx, bootCancel := context.WithTimeout(context.Background(), 5*time.Second)
			email, password, err := api.BootstrapAdminIfEmpty(bootCtx, database)
			bootCancel()
			if err != nil {
				logger.Error("First-boot bootstrap failed", "error", err)
				os.Exit(1)
			}
			dashURL := dashboardURL(cfg)
			if email != "" {
				logger.Info(
					"First-boot bootstrap created an admin account — capture the temporary password now and log in to change it.",
					"url", dashURL,
					"email", email,
					"temporary_password", password,
				)
				// Plain-text banner on stderr — the structured log line
				// above is for log aggregators, this is for the human
				// watching `docker compose up`. Easy to miss the JSON
				// otherwise when 30+ log lines fly by during boot.
				printBootstrapBanner(dashURL, email, password)
			} else {
				logger.Info("Admin dashboard ready", "url", dashURL)
			}

			projCtx, projCancel := context.WithTimeout(context.Background(), 5*time.Second)
			created, err := api.BootstrapDefaultProjectIfEmpty(projCtx, database)
			projCancel()
			if err != nil {
				logger.Error("Default-project bootstrap failed", "error", err)
				os.Exit(1)
			}
			if created {
				logger.Info(
					"Bootstrapped default project (Firebase-compat, wide-open rules) — edit or delete it in the dashboard.",
					"project_id", api.BootstrapProjectID,
				)
			}
		}
	}
	defer database.Close()

	// Create proxy server (with API handler for consolidated HTTPS)
	proxyServer, err := proxy.New(cfg, database, pool, apiServer)
	if err != nil {
		logger.Error("Failed to create proxy server", "error", err)
		os.Exit(1)
	}

	// Enable emulator mode for auth in local mode
	if cfg.LocalMode {
		proxyServer.SetEmulatorMode(true)
	}

	// Build the notify dispatcher whenever we have a real database — both
	// the LISTEN loop (Postgres) and the admin write paths (any driver)
	// fan out through it.
	var notifyListener *notify.Listener
	if database != nil && configAdapter != nil {
		dispatcher := notify.NewDispatcher(database, pool, configAdapter, proxyServer)
		apiServer.SetNotifyHandler(dispatcher)

		if database.DriverKind() == db.DriverPostgres {
			notifyListener = notify.NewListener(database, dispatcher)
			notifyListener.Start()
			logger.Info("Postgres NOTIFY listener started", "channels", []string{notify.ChannelProjectConfigChanged, notify.ChannelDatabaseEvicted})
		}
	}

	// Setup graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Start all services
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Start proxy server (handles both client connections and API on same port)
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Proxy server starting")
		if err := proxyServer.Run(); err != nil {
			errCh <- err
		}
	}()

	// Start cleanup job (not in local mode)
	if cleanup != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("Cleanup job starting")
			cleanup.Run()
		}()
	}

	// Start metrics aggregator (not in local mode)
	if metricsAggregator != nil {
		metricsAggregator.Start()
		logger.Info("Metrics aggregator started")
	}

	// Start stats reporter
	go statsReporter(proxyServer, pool)

	// Wait for shutdown signal or error
	select {
	case sig := <-shutdown:
		logger.Info("Received signal, shutting down", "signal", sig)
	case err := <-errCh:
		logger.Error("Service error", "error", err)
	}

	// Graceful shutdown
	logger.Info("Initiating graceful shutdown")

	if metricsAggregator != nil {
		logger.Debug("Stopping metrics aggregator")
		metricsAggregator.Stop()
	}
	if cleanup != nil {
		cleanup.Stop()
	}
	if notifyListener != nil {
		notifyListener.Stop()
	}
	proxyServer.Shutdown()

	// Wait for all services to stop (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All services stopped")
	case <-time.After(10 * time.Second):
		logger.Warn("Shutdown timeout, forcing exit")
	}
}

func statsReporter(proxyServer *proxy.Server, pool *backend.Pool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// Get queue depths
		queueStats := pool.GetQueueStats()
		var queueInfo string
		for id, qs := range queueStats {
			queueInfo += fmt.Sprintf(" %s[inbox=%d/%d]", id, qs.InboxLen, qs.InboxCap)
		}

		logger.Debug("STATS",
			"connections", proxyServer.ConnectionCount(),
			"mem_mb", m.Alloc/1024/1024,
			"goroutines", runtime.NumGoroutine(),
			"queues", queueInfo,
		)
	}
}

// serverDiscoveryAdapter adapts db.Store to backend.ServerDiscovery interface
type serverDiscoveryAdapter struct {
	db db.Store
}

func (a *serverDiscoveryAdapter) GetServersForDiscovery() ([]backend.ServerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	servers, err := a.db.GetAllServersForDiscovery(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]backend.ServerInfo, 0, len(servers))
	for _, s := range servers {
		// Skip servers without proxy port configured
		if s.ProxyPort == 0 {
			continue
		}
		result = append(result, backend.ServerInfo{
			ServerID: s.ID,
			Address:  s.ProxyAddress(),
		})
	}

	return result, nil
}

// configProviderAdapter adapts db.Store to backend.ConfigProvider interface
type configProviderAdapter struct {
	db db.Store
}

func (a *configProviderAdapter) GetProjectConfig(projectID string) (*backend.ProjectConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	project, err := a.db.GetProjectByID(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &backend.ProjectConfig{
		Rules:             project.RulesJSON,
		SecretKey:         project.SecretKey,
		AdminSecretKey:    project.AdminSecretKey,
		FirebaseProjectID: project.FirebaseProjectID,
		Ephemeral:         project.Ephemeral,
		ConfigVersion:     project.ConfigVersion,
		Settings:          nil,
	}, nil
}

func (a *configProviderAdapter) UpdateServerHeartbeat(serverID string, load int, clients int, memMB int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return a.db.UpdateServerHeartbeatFromProxy(ctx, serverID, load, clients, memMB)
}

func (a *configProviderAdapter) EvictDatabases(evictions []backend.EvictionRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Longer timeout for batch
	defer cancel()

	// Convert to db package type
	dbEvictions := make([]db.EvictionRequest, len(evictions))
	for i, e := range evictions {
		dbEvictions[i] = db.EvictionRequest{
			ProjectID:  e.ProjectID,
			DatabaseID: e.DatabaseID,
		}
	}

	return a.db.EvictDatabases(ctx, dbEvictions)
}

// dashboardURL returns a best-guess URL operators can paste into a
// browser to reach /admin/. It's built from LARKDB_DOMAIN + the main
// listener's port + http/https (from DISABLE_TLS). Default ports (80/443)
// are omitted from the printed URL. Always falls back to a working host.
func dashboardURL(cfg *config.Config) string {
	scheme := "https"
	if cfg.DisableTLS {
		scheme = "http"
	}

	host := cfg.LarkDBDomain
	if host == "" {
		host = "localhost"
	}

	// HTTPSListenAddr is something like ":8080" or "0.0.0.0:8080".
	_, port, err := net.SplitHostPort(cfg.HTTPSListenAddr)
	if err != nil {
		// Treat the whole value as a port, stripping any leading ":".
		port = strings.TrimPrefix(cfg.HTTPSListenAddr, ":")
	}

	portSuffix := ""
	if port != "" && !(scheme == "http" && port == "80") && !(scheme == "https" && port == "443") {
		portSuffix = ":" + port
	}

	return fmt.Sprintf("%s://%s%s/admin/", scheme, host, portSuffix)
}

// printBootstrapBanner writes a multi-line plain-text banner to stderr
// alongside the structured-JSON bootstrap log line. The JSON line is for
// log aggregators; this banner is for whoever is watching `docker compose
// up` in a terminal — the temp password is hard to spot in 30+ log lines
// of JSON otherwise. stderr keeps it out of stdout-only log captures.
func printBootstrapBanner(dashURL, email, password string) {
	const bar = "================================================================================"
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, bar)
	fmt.Fprintln(os.Stderr, "  LARK FIRST-BOOT BOOTSTRAP")
	fmt.Fprintln(os.Stderr, bar)
	fmt.Fprintf(os.Stderr, "  Dashboard:           %s\n", dashURL)
	fmt.Fprintf(os.Stderr, "  Email:               %s\n", email)
	fmt.Fprintf(os.Stderr, "  Temporary password:  %s\n", password)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  Capture the password now — it's only printed once. After login")
	fmt.Fprintln(os.Stderr, "  you'll be required to change it.")
	fmt.Fprintln(os.Stderr, bar)
	fmt.Fprintln(os.Stderr)
}

