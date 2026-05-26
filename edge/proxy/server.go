// Server Implementation
//
// This file implements the main proxy server that listens for client connections
// and routes them to backend servers. It handles:
//   - TLS termination (via CertMagic or static certificates)
//   - Multiple transport protocols (WebSocket, WebTransport, REST, SSE, Long Polling)
//   - Domain-based routing (*.larkdb.net → clients, db.lark.sh → admin API)
//   - Client lifecycle management
//
// # Listeners
//
// The server runs multiple listeners:
//   - HTTPS (default :443): WebSocket, REST API, Long Polling, SSE
//   - WebTransport (default :8444): QUIC/HTTP3 for low-latency connections
//
// Multiple WebTransport listeners can run on consecutive ports for load distribution.
//
// # Domain Routing
//
// Requests are routed based on the Host header:
//   - *.larkdb.net: Client-facing endpoints (handleLarkDB)
//   - Other domains: Admin API (apiHandler from api package)
//
// # TLS Configuration
//
// Two modes are supported:
//   - CertMagic: Automatic Let's Encrypt certificates via DNS-01 challenge
//   - Static: Load certificates from files (for development or custom certs)
//
// In local mode (LOCAL_MODE=true), TLS is disabled entirely.
//
// # Client Management
//
// Clients are tracked in a sync.Map by their 32-bit ID. The ID space is limited
// to 65,535 concurrent connections per proxy (to fit in backend wire protocol).
// IDs are recycled when clients disconnect.
//
// # Integration Points
//
//   - backend.Pool: For sending messages to backend servers
//   - backend.ClientRegistry: Implemented here to route responses to clients
//   - backend.ClientNotifier: Implemented here to close clients on backend failures
//   - api.Server: Mounted as apiHandler for non-larkdb domains
package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lark-sh/lark/edge/auth"
	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"

	"github.com/bytedance/sonic"
	"github.com/caddyserver/certmagic"
	"github.com/gorilla/websocket"
	"github.com/libdns/cloudflare"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// Server handles client connections and proxies them to backends
type Server struct {
	config *config.Config
	db     db.Store
	pool   *backend.Pool

	// TLS
	tlsConfig *tls.Config

	// Auth validator (supports Lark tokens, Firebase ID tokens, Firebase custom tokens)
	authValidator *auth.MultiValidator

	// Client tracking
	clients   sync.Map // clientID -> *ClientConn
	nextID    atomic.Uint32
	freeIDs   []uint32   // recycled client IDs
	freeIDsMu sync.Mutex

	// Connection metrics (for proxy_metrics emission)
	wsConnections  atomic.Int32 // WebSocket connections
	wtConnections  atomic.Int32 // WebTransport connections
	lpConnections  atomic.Int32 // Long Poll connections
	sseConnections atomic.Int32 // SSE connections
	restRequests   atomic.Int64 // REST requests (counter, reset on emit)

	// REST client pool (virtual clients for REST requests)
	restPool *RESTClientPool

	// Long Poll session pool (for Firebase Long Polling)
	lpPool *LongPollPool

	// WebSocket upgrader
	wsUpgrader websocket.Upgrader

	// WebTransport
	wtServer *webtransport.Server

	// Project config cache (keyed by project ID, lazily expires after 30s)
	projectCache sync.Map // projectID -> *projectCacheEntry

	// API handler (for coordinator endpoints, admin, etc.)
	apiHandler http.Handler

	// Shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// projectCacheEntry holds a cached project config with TTL
type projectCacheEntry struct {
	project   *db.Project
	fetchedAt time.Time
}

const projectCacheTTL = 30 * time.Second

// GetProjectCached returns a project config, using a TTL cache to avoid repeated DB queries.
// Safe for concurrent use. Multiple goroutines may race to populate the same key; that's fine.
func (s *Server) GetProjectCached(ctx context.Context, projectID string) (*db.Project, error) {
	if val, ok := s.projectCache.Load(projectID); ok {
		entry := val.(*projectCacheEntry)
		if time.Since(entry.fetchedAt) < projectCacheTTL {
			return entry.project, nil
		}
	}

	project, err := s.db.GetProjectByID(ctx, projectID)
	if err != nil {
		return nil, err
	}

	s.projectCache.Store(projectID, &projectCacheEntry{
		project:   project,
		fetchedAt: time.Now(),
	})
	return project, nil
}

// InvalidateProjectCache drops the cached project config so the next read
// re-fetches from the database. Called when a project_config_changed NOTIFY
// arrives from Postgres.
func (s *Server) InvalidateProjectCache(projectID string) {
	s.projectCache.Delete(projectID)
}

// clientRegistry wraps Server to implement backend.ClientRegistry
type clientRegistry struct {
	server *Server
}

// GetClient implements backend.ClientRegistry
func (r *clientRegistry) GetClient(clientID uint32) backend.Client {
	if val, ok := r.server.clients.Load(clientID); ok {
		return val.(*ClientConn)
	}
	return nil
}

// clientNotifier wraps Server to implement backend.ClientNotifier
type clientNotifier struct {
	server *Server
}

// OnDatabaseUnloaded implements backend.ClientNotifier
// Closes all clients connected to the specified database via the specified backend
func (n *clientNotifier) OnDatabaseUnloaded(serverID, projectID, databaseID string) {
	var toClose []*ClientConn

	n.server.clients.Range(func(key, value interface{}) bool {
		client := value.(*ClientConn)
		// Match clients that are:
		// 1. Connected to this project/database
		// 2. Using this specific backend (serverID)
		if client.projectID == projectID &&
			client.databaseID == databaseID &&
			client.backend != nil &&
			client.backend.ServerID == serverID {
			toClose = append(toClose, client)
		}
		return true
	})

	if len(toClose) > 0 {
		logger.Debug("Closing clients for unloaded database", "count", len(toClose), "project_id", projectID, "database_id", databaseID, "server_id", serverID)
		for _, client := range toClose {
			client.Close()
		}
	}
}

// OnBackendDisconnected implements backend.ClientNotifier
// Closes all clients that were using the specified backend
func (n *clientNotifier) OnBackendDisconnected(serverID string) {
	var toClose []*ClientConn

	n.server.clients.Range(func(key, value interface{}) bool {
		client := value.(*ClientConn)
		// Match clients using this backend
		if client.backend != nil && client.backend.ServerID == serverID {
			toClose = append(toClose, client)
		}
		return true
	})

	if len(toClose) > 0 {
		logger.Debug("Closing clients for disconnected backend", "count", len(toClose), "server_id", serverID)
		for _, client := range toClose {
			client.Close()
		}
	}
}

// New creates a new proxy server
func New(cfg *config.Config, database db.Store, pool *backend.Pool, apiHandler http.Handler) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create auth validator (Firebase project IDs are added dynamically per-project)
	authValidator := auth.NewMultiValidator(nil)

	s := &Server{
		config:        cfg,
		db:            database,
		pool:          pool,
		authValidator: authValidator,
		apiHandler:    apiHandler,
		ctx:           ctx,
		cancel:        cancel,
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}

	// Skip TLS setup when DISABLE_TLS=true (which LOCAL_MODE implies).
	if !cfg.DisableTLS {
		if err := s.setupTLS(); err != nil {
			cancel()
			return nil, fmt.Errorf("setup TLS: %w", err)
		}
	}

	// Set up client registry for backend response routing
	pool.SetClientRegistry(&clientRegistry{server: s})

	// Set up client notifier for backend events (database unloaded, backend disconnected)
	pool.SetClientNotifier(&clientNotifier{server: s})

	// Create REST client pool for virtual REST clients
	s.restPool = NewRESTClientPool(s, 10*time.Second)

	// Create Long Poll session pool
	s.lpPool = NewLongPollPool(s, 60*time.Second) // 60s idle timeout for LP sessions

	return s, nil
}

// setupTLS configures TLS using either CertMagic or static certs
func (s *Server) setupTLS() error {
	if s.config.CertMagicEnabled {
		return s.setupCertMagic()
	}
	return s.setupStaticTLS()
}

// setupCertMagic configures automatic certificate management
func (s *Server) setupCertMagic() error {
	// Configure Cloudflare DNS provider
	if s.config.CloudflareAPIToken == "" {
		return fmt.Errorf("CLOUDFLARE_API_TOKEN required when CertMagic is enabled")
	}

	cfProvider := &cloudflare.Provider{
		APIToken: s.config.CloudflareAPIToken,
	}

	// Configure CertMagic
	certmagic.DefaultACME.Agreed = true
	certmagic.DefaultACME.Email = s.config.CertMagicEmail
	certmagic.DefaultACME.DNS01Solver = &certmagic.DNS01Solver{
		DNSManager: certmagic.DNSManager{
			DNSProvider: cfProvider,
			Resolvers:   s.config.CertMagicResolvers,
		},
	}

	// Use Let's Encrypt staging environment for testing (certs won't be browser-trusted)
	if s.config.CertMagicStaging {
		certmagic.DefaultACME.CA = certmagic.LetsEncryptStagingCA
		logger.Info("CertMagic using Let's Encrypt STAGING environment")
	}

	// Set storage path
	certmagic.Default.Storage = &certmagic.FileStorage{Path: s.config.CertMagicStoragePath}

	// Create config for our domains
	magic := certmagic.NewDefault()

	// Manage certificates for our domains
	err := magic.ManageSync(s.ctx, s.config.CertMagicDomains)
	if err != nil {
		return fmt.Errorf("certmagic manage: %w", err)
	}

	s.tlsConfig = magic.TLSConfig()
	s.tlsConfig.NextProtos = []string{"h3", "h2", "http/1.1"}

	logger.Info("CertMagic enabled", "domains", s.config.CertMagicDomains)
	return nil
}

// setupStaticTLS loads certificates from files
func (s *Server) setupStaticTLS() error {
	cert, err := tls.LoadX509KeyPair(s.config.TLSCertFile, s.config.TLSKeyFile)
	if err != nil {
		return fmt.Errorf("load certificate: %w", err)
	}

	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h3", "h2", "http/1.1"},
		MinVersion:   tls.VersionTLS12,
	}

	logger.Info("Static TLS enabled", "cert_file", s.config.TLSCertFile)
	return nil
}

// Run starts all listeners
func (s *Server) Run() error {
	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// Start proxy metrics emission goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.emitProxyMetricsLoop()
	}()

	// Start internal HTTP listener (for server-to-server traffic)
	// Skip in local mode - not needed and would conflict with main HTTP server
	if s.config.InternalListenAddr != "" && !s.config.LocalMode {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.runInternalHTTP(); err != nil {
				errCh <- fmt.Errorf("internal http: %w", err)
			}
		}()
	}

	// Start WebSocket listener
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.runWebSocket(); err != nil {
			errCh <- fmt.Errorf("websocket: %w", err)
		}
	}()

	// WebTransport requires TLS (QUIC), so skip when TLS is disabled.
	if !s.config.DisableTLS {
		for i := 0; i < s.config.WebTransportPorts; i++ {
			port := 8444 + i // Base port + offset
			if s.config.WebTransportListenAddr != "" {
				// Parse base port from config
				_, portStr, _ := net.SplitHostPort(s.config.WebTransportListenAddr)
				if portStr != "" {
					fmt.Sscanf(portStr, "%d", &port)
					port += i
				}
			}

			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				if err := s.runWebTransport(p); err != nil {
					errCh <- fmt.Errorf("webtransport port %d: %w", p, err)
				}
			}(port)
		}
	} else {
		logger.Info("WebTransport disabled (requires TLS)")
	}

	// Wait for shutdown or error
	select {
	case <-s.ctx.Done():
		logger.Info("Shutting down")
	case err := <-errCh:
		logger.Error("Proxy error", "error", err)
		s.cancel()
		return err
	}

	wg.Wait()
	return nil
}

// runWebSocket starts the main HTTP/HTTPS listener (WebSocket + API)
func (s *Server) runWebSocket() error {
	server := &http.Server{
		Handler: http.HandlerFunc(s.routeByHost),
	}

	go func() {
		<-s.ctx.Done()
		server.Close()
	}()

	// When TLS is disabled, serve plain HTTP. Browsers exempt localhost
	// from mixed-content rules, so this is the path for the default
	// docker compose dev story.
	if s.config.DisableTLS {
		listener, err := net.Listen("tcp", s.config.HTTPSListenAddr)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
		defer listener.Close()

		logger.Info("HTTP server listening (TLS disabled)", "addr", s.config.HTTPSListenAddr)
		return server.Serve(listener)
	}

	// Production mode: use TLS
	// TODO: Enable kTLS for WebSocket connections on Linux
	// - Replace crypto/tls with github.com/secure-for-ai/goktls
	// - Need to handle CertMagic config conversion (CertMagic returns *crypto/tls.Config,
	//   goktls uses its own *tls.Config type - need to copy GetCertificate callback)
	// - Set GOKTLS=1 environment variable in production
	// - kTLS offloads TLS encryption to kernel, enabling zero-copy splice()
	listener, err := tls.Listen("tcp", s.config.HTTPSListenAddr, s.tlsConfig)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	logger.Info("HTTPS server listening", "addr", s.config.HTTPSListenAddr)
	return server.Serve(listener)
}

// runInternalHTTP starts a plain HTTP listener for internal server-to-server traffic.
// This is used for metrics ingestion and other internal APIs.
// Should be firewalled to only allow internal network traffic (e.g., 10.0.0.0/8).
func (s *Server) runInternalHTTP() error {
	server := &http.Server{
		Handler: s.apiHandler,
	}

	go func() {
		<-s.ctx.Done()
		server.Close()
	}()

	listener, err := net.Listen("tcp", s.config.InternalListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	logger.Info("Internal HTTP server listening", "addr", s.config.InternalListenAddr)
	return server.Serve(listener)
}

// routeByHost routes requests based on the Host header
// - *.larkdb.net → client-facing (WebSocket, REST API to databases)
// - db.lark.sh (or other) → coordinator/admin API
func (s *Server) routeByHost(w http.ResponseWriter, r *http.Request) {
	if s.isLarkDBDomain(r.Host) {
		// Client-facing: *.larkdb.net
		// Permissive CORS is OK here because auth is token-based (not cookies)
		// Tokens are stored in localStorage which is same-origin protected
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}

		// Handle preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		s.handleLarkDB(w, r)
	} else {
		// Coordinator/Admin API: db.lark.sh or other domains
		// CORS is handled by apiHandler with restricted origin allowlist
		// (uses session cookies, so must restrict to prevent CSRF)

		// Block /internal/* routes on the public server - these are only
		// accessible via the internal HTTP server (port 8080)
		if strings.HasPrefix(r.URL.Path, "/internal/") {
			http.NotFound(w, r)
			return
		}

		if s.apiHandler != nil {
			s.apiHandler.ServeHTTP(w, r)
		} else {
			http.NotFound(w, r)
		}
	}
}

// runWebTransport starts a WebTransport listener on the given port
func (s *Server) runWebTransport(port int) error {
	addr := fmt.Sprintf(":%d", port)

	// Create UDP listener (IPv4 only - GSO issues with IPv6)
	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return fmt.Errorf("resolve udp addr: %w", err)
	}

	udpConn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}
	defer udpConn.Close()

	// Create WebTransport server
	wtServer := &webtransport.Server{
		H3: &http3.Server{
			TLSConfig:       s.tlsConfig,
			EnableDatagrams: true,
			QUICConfig: &quic.Config{
				EnableDatagrams: true,
			},
		},
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	s.wtServer = wtServer

	// HTTP handler for WebTransport
	mux := http.NewServeMux()
	mux.HandleFunc("/wt", func(w http.ResponseWriter, r *http.Request) {
		// Extract project ID (and optional database ID) from host before upgrading
		projectID, subdomainDB := s.extractProjectAndDatabase(r.Host)
		if projectID == "" {
			logger.Warn("WT no project ID in request", "host", r.Host)
			http.Error(w, "invalid host", http.StatusBadRequest)
			return
		}

		session, err := wtServer.Upgrade(w, r)
		if err != nil {
			logger.Warn("WT upgrade error", "error", err)
			return
		}
		// Handle session in goroutine so HTTP handler can return
		// (required for CONNECT response to complete)
		go s.handleWebTransportSession(session, port, projectID, subdomainDB)
	})
	wtServer.H3.Handler = mux

	logger.Info("WebTransport listening", "addr", addr, "protocol", "UDP")

	go func() {
		<-s.ctx.Done()
		wtServer.Close()
	}()

	return wtServer.Serve(udpConn)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() {
	s.cancel()

	// Close all client connections
	s.clients.Range(func(key, value interface{}) bool {
		if client, ok := value.(*ClientConn); ok {
			client.Close()
		}
		return true
	})
}

// MaxClientID is the maximum client ID per proxy (16-bit range for backend optimization)
const MaxClientID = 65535

// allocateClientID returns a unique client ID, recycling freed IDs when possible.
// Returns 0 if no IDs are available (too many concurrent connections).
func (s *Server) allocateClientID() uint32 {
	// First try the free list (recycled IDs from disconnected clients)
	s.freeIDsMu.Lock()
	if len(s.freeIDs) > 0 {
		id := s.freeIDs[len(s.freeIDs)-1]
		s.freeIDs = s.freeIDs[:len(s.freeIDs)-1]
		s.freeIDsMu.Unlock()
		return id
	}
	s.freeIDsMu.Unlock()

	// No free IDs, allocate new one
	id := s.nextID.Add(1)
	if id > MaxClientID {
		// Exhausted ID space - too many concurrent connections
		return 0
	}
	return id
}

// registerClient adds a client to the tracking map
func (s *Server) registerClient(client *ClientConn) {
	s.clients.Store(client.id, client)

	// Update transport-specific connection counter
	switch client.transport.(type) {
	case *WebSocketTransport:
		s.wsConnections.Add(1)
	case *WebTransportConn:
		s.wtConnections.Add(1)
	case *LongPollTransport:
		s.lpConnections.Add(1)
	case *SSETransport:
		s.sseConnections.Add(1)
	}
}

// unregisterClient removes a client from the tracking map and recycles its ID
func (s *Server) unregisterClient(client *ClientConn) {
	s.clients.Delete(client.id)

	// Update transport-specific connection counter
	switch client.transport.(type) {
	case *WebSocketTransport:
		s.wsConnections.Add(-1)
	case *WebTransportConn:
		s.wtConnections.Add(-1)
	case *LongPollTransport:
		s.lpConnections.Add(-1)
	case *SSETransport:
		s.sseConnections.Add(-1)
	}

	// Return ID to free list for reuse
	s.freeIDsMu.Lock()
	s.freeIDs = append(s.freeIDs, client.id)
	s.freeIDsMu.Unlock()
}

// getClient returns a client by ID
func (s *Server) getClient(id uint32) *ClientConn {
	if val, ok := s.clients.Load(id); ok {
		return val.(*ClientConn)
	}
	return nil
}

// ConnectionCount returns the number of active connections
func (s *Server) ConnectionCount() int {
	count := 0
	s.clients.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// SetEmulatorMode enables or disables emulator mode for auth.
// In emulator mode, the token "owner" is accepted and grants admin access.
func (s *Server) SetEmulatorMode(enabled bool) {
	if s.authValidator != nil {
		s.authValidator.SetEmulatorMode(enabled)
	}
}

// emitProxyMetricsLoop emits proxy metrics to stdout every 60 seconds as
// line-delimited JSON, suitable for scraping by any log/metric collector.
func (s *Server) emitProxyMetricsLoop() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.emitProxyMetrics(hostname)
		}
	}
}

// emitProxyMetrics outputs proxy_metrics JSON to stdout
func (s *Server) emitProxyMetrics(hostname string) {
	// Swap REST requests counter to get delta since last emission
	restRequests := s.restRequests.Swap(0)

	metrics := map[string]interface{}{
		"type":            "proxy_metrics",
		"ts":              time.Now().Unix(),
		"proxy":           hostname,
		"ws_connections":  s.wsConnections.Load(),
		"wt_connections":  s.wtConnections.Load(),
		"lp_connections":  s.lpConnections.Load(),
		"sse_connections": s.sseConnections.Load(),
		"rest_requests":   restRequests,
	}

	data, err := sonic.Marshal(metrics)
	if err != nil {
		logger.Error("Failed to marshal proxy metrics", "error", err)
		return
	}

	fmt.Println(string(data))
}
