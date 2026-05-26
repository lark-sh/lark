// Package backend manages connections to backend Lark database servers.
//
// # Architecture Overview
//
// The proxy maintains a pool of TCP connections to each backend server. Messages from
// clients are multiplexed over these connections using a custom wire protocol. This
// allows thousands of client connections to share a smaller number of backend connections.
//
// Each backend server runs multiple cores (isolated database processes). The pool routes
// messages to the correct core based on a hash of project_id/database_id. This ensures
// all messages for a given database always go to the same core.
//
// # Connection Management
//
// For each backend server, the pool maintains `connsPerCore` TCP connections to each core.
// The default is 2 connections per core (64 cores = 128 connections per backend). Connections
// are established lazily on first use and automatically reconnected on failure.
//
// # Message Flow (Client → Backend)
//
// 1. Client calls pool.SendMessage(serverID, clientID, coreID, payload)
// 2. Message goes into the backend's inbox channel (100k buffer)
// 3. Single batcherLoop goroutine accumulates messages into batches
// 4. Batch is flushed every 3ms or when it reaches 1MB
// 5. flushBatch writes to all backend connections in parallel
// 6. Messages are sharded by clientID % connsPerCore for consistent ordering
//
// # Message Flow (Backend → Client)
//
// 1. Each connection has a readLoop that receives wire protocol messages
// 2. Client-bound messages are dispatched inline in readLoop:
//    - Look up client via ClientRegistry
//    - Call client.Deliver() (non-blocking)
// 3. Control messages (HEARTBEAT, DATABASE_LOADED, etc.) go to controlChan
//
// # Eviction Batching
//
// When backends evict databases (DATABASE_UNLOADED), all proxies in the mesh receive the
// message. To avoid hammering Postgres, evictions are batched: 1 second delay or 500 items,
// whichever comes first. The batch query is idempotent, so multiple proxies processing the
// same eviction has no ill effects.
//
// # Key Interfaces
//
//   - ClientRegistry: Allows the pool to look up clients for response routing
//   - ConfigProvider: Provides project configs and handles heartbeat/eviction updates
//   - ClientNotifier: Notifies the proxy when backends/databases become unavailable
//
// # Thread Safety
//
// The Pool is fully thread-safe. All public methods can be called concurrently from any
// goroutine. Internal synchronization uses a combination of mutexes (for backend map
// access) and lock-free channels (for message passing).
package backend

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/lark-sh/lark/edge/logger"
)

var (
	ErrPoolClosed      = errors.New("connection pool is closed")
	ErrNoConnections   = errors.New("no backend connections available")
	ErrCoreUnavailable = errors.New("no connections available for target core")
	ErrClientNotRouted = errors.New("client not routed to a core")
)

// ClientRegistry is the interface the pool uses to route responses to clients.
//
// Thread Safety: Implementations MUST be safe for concurrent access from multiple
// goroutines. The pool calls GetClient from dispatch workers in parallel.
//
// Lifecycle: The registry should return nil for clients that have disconnected.
// The pool handles nil gracefully (logs and drops the message).
type ClientRegistry interface {
	// GetClient looks up a client by its 32-bit ID.
	// Returns nil if the client is not connected or has been closed.
	// Must be safe for concurrent calls from multiple goroutines.
	GetClient(clientID uint32) Client
}

// Client is the interface for a client that can receive messages from the backend.
//
// Thread Safety: Deliver and Close MUST be safe for concurrent calls.
// The pool may call Deliver from multiple dispatch workers simultaneously.
type Client interface {
	// Deliver delivers a message to the client's outbox.
	//
	// This is non-blocking: if the client's buffer is full, the message is dropped
	// and false is returned. The pool does not retry - the client is expected to
	// reconnect if it falls too far behind.
	//
	// The reliable flag indicates whether this message requires guaranteed delivery:
	//   - true: Data messages, acknowledgments (TCP semantics)
	//   - false: Volatile/ephemeral data (can use UDP/datagrams if available)
	//
	// The payload is owned by the caller and must not be modified after this call.
	Deliver(payload []byte, reliable bool) bool

	// Close terminates the client connection.
	// Safe to call multiple times (subsequent calls are no-ops).
	// Must not block; actual cleanup can happen asynchronously.
	Close()
}

// ConfigProvider is the interface for database operations the pool needs.
//
// Thread Safety: All methods MUST be safe for concurrent access.
// The pool calls these from multiple goroutines (heartbeat handlers, eviction batcher).
//
// Implementation: Typically backed by Postgres (db.DB) but can be mocked for testing.
type ConfigProvider interface {
	// GetProjectConfig fetches a project's configuration for pushing to backend servers.
	// Called when a backend requests configuration (CONFIG_REQUEST message).
	// Returns error if project doesn't exist or database is unavailable.
	GetProjectConfig(projectID string) (*ProjectConfig, error)

	// UpdateServerHeartbeat updates the server's health metrics in the database.
	// Called periodically when HEARTBEAT messages arrive from backends.
	// This keeps the server marked as "healthy" for routing decisions.
	//
	// Parameters are aggregated across all cores:
	//   - load: Average load percentage (0-100)
	//   - clients: Total connected clients
	//   - memMB: Total memory usage in MB
	UpdateServerHeartbeat(serverID string, load int, clients int, memMB int) error

	// EvictDatabases handles batch database eviction from routing tables.
	// Called by the eviction batcher when DATABASE_UNLOADED messages arrive.
	//
	// For each eviction:
	//   - Ephemeral databases: Deleted from the database
	//   - Persistent databases: server_id cleared, status set to 'inactive'
	//
	// The operation is idempotent - multiple proxies can evict the same databases.
	EvictDatabases(evictions []EvictionRequest) error
}

// ClientNotifier is the interface for notifying the proxy when clients need to be
// disconnected due to backend events.
//
// Thread Safety: All methods MUST be safe for concurrent calls.
// The pool may call these from connection death handlers and message processors.
//
// Purpose: When a backend becomes unavailable or evicts a database, connected clients
// become "orphaned" - they have an open connection but the backend knows nothing about
// them. Closing these clients forces a reconnect, which routes them to a healthy backend.
type ClientNotifier interface {
	// OnDatabaseUnloaded is called when a backend evicts a specific database.
	//
	// This happens when:
	//   - Database idle timeout expires
	//   - Memory pressure forces eviction
	//   - Administrative eviction request
	//
	// The proxy should close all clients that:
	//   - Are connected to the specified project/database
	//   - Were routed to the specified backend (serverID)
	//
	// Clients connected to the same database via a different backend are NOT affected.
	OnDatabaseUnloaded(serverID, projectID, databaseID string)

	// OnBackendDisconnected is called when a backend becomes completely unavailable.
	//
	// This happens when:
	//   - All TCP connections to the backend have died
	//   - Backend.Close() is called
	//
	// The proxy should close ALL clients that were using this backend, regardless
	// of which project/database they were connected to.
	OnBackendDisconnected(serverID string)
}

// EvictionRequest represents a single database eviction
type EvictionRequest struct {
	ProjectID  string
	DatabaseID string
}

// CoreMetrics holds metrics for a single core
type CoreMetrics struct {
	Load      uint16
	Clients   uint32
	MemMB     uint32
	LastSeen  time.Time
}

// Pool manages connections to backend servers
type Pool struct {
	mu       sync.RWMutex
	backends map[string]*Backend // serverID -> Backend
	closed   bool

	// Client registry for routing responses
	clients ClientRegistry

	// Config provider for project configs and heartbeat updates
	configProvider ConfigProvider

	// Client notifier for backend events that require client disconnection
	clientNotifier ClientNotifier

	// Config
	connectTimeout time.Duration
	writeTimeout   time.Duration
	readTimeout    time.Duration
	connsPerCore   int // Number of connections per core (default: 2)
}

// NewPool creates a new backend connection pool
func NewPool(connsPerCore int) *Pool {
	if connsPerCore <= 0 {
		connsPerCore = 2 // default
	}
	return &Pool{
		backends:       make(map[string]*Backend),
		connectTimeout: 5 * time.Second,
		writeTimeout:   10 * time.Second,
		readTimeout:    30 * time.Second,
		connsPerCore:   connsPerCore,
	}
}

// SetClientRegistry sets the client registry for routing responses
func (p *Pool) SetClientRegistry(clients ClientRegistry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients = clients
}

// SetConfigProvider sets the config provider for project configs and heartbeat updates
func (p *Pool) SetConfigProvider(provider ConfigProvider) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.configProvider = provider
}

// SetClientNotifier sets the client notifier for backend events
func (p *Pool) SetClientNotifier(notifier ClientNotifier) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clientNotifier = notifier
}

// AddBackend adds a backend server to the pool
// Establishes connections and learns the core topology from HELLO_ACK handshakes
func (p *Pool) AddBackend(serverID, address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	// First, establish one connection to learn the server topology
	dialer := net.Dialer{Timeout: p.connectTimeout}
	netConn, err := dialer.Dial("tcp", address)
	if err != nil {
		return err
	}

	// Perform handshake to learn nrCores
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		tcpConn.SetWriteBuffer(2 * 1024 * 1024)
	}

	if err := WriteHello(netConn, ProxyVersion); err != nil {
		netConn.Close()
		return err
	}

	helloAck, err := ReadHelloAck(netConn)
	if err != nil {
		netConn.Close()
		return err
	}

	nrCores := int(helloAck.NrCores)
	firstCoreID := int(helloAck.CoreID)

	logger.Debug("Server topology discovered", "server_id", serverID, "cores", nrCores, "first_core", firstCoreID)

	backend := &Backend{
		ServerID:       serverID,
		Address:        address,
		nrCores:        nrCores,
		coreConns:      make([][]*Conn, nrCores),
		coreNext:       make([]int, nrCores),
		clientToCore:   make(map[uint32]int),
		coreProjects:   make([]map[string]bool, nrCores),
		projectCores:   make(map[string]map[int]bool),
		coreMetrics:    make([]CoreMetrics, nrCores),
		pool:         p,
		inbox:        make(chan *inboxMessage, 3000000),
		controlChan:  make(chan *ControlMessage, 10000),
		evictionChan: make(chan EvictionRequest, 10000),
		done:         make(chan struct{}),
	}

	// Initialize per-core slices
	for i := 0; i < nrCores; i++ {
		backend.coreConns[i] = make([]*Conn, 0, p.connsPerCore)
		backend.coreProjects[i] = make(map[string]bool)
	}

	// Add the first connection we already established
	firstConn := NewConn(netConn, backend, firstCoreID)
	backend.coreConns[firstCoreID] = append(backend.coreConns[firstCoreID], firstConn)
	go firstConn.readLoop()

	// Establish remaining connections (connsPerCore per core)
	// We keep connecting until each core has connsPerCore connections
	for {
		// Check if all cores have enough connections
		allFull := true
		for coreID := 0; coreID < nrCores; coreID++ {
			if len(backend.coreConns[coreID]) < p.connsPerCore {
				allFull = false
				break
			}
		}
		if allFull {
			break
		}

		// Establish another connection
		conn, coreID, err := backend.connectWithHandshake()
		if err != nil {
			logger.Error("Failed to connect to backend", "server_id", serverID, "error", err)
			continue
		}

		// Only add if this core still needs connections
		if len(backend.coreConns[coreID]) < p.connsPerCore {
			backend.coreConns[coreID] = append(backend.coreConns[coreID], conn)
			go conn.readLoop()
		} else {
			// Core is full, close this connection
			conn.Close()
		}
	}

	// Count total connections
	totalConns := 0
	for coreID := 0; coreID < nrCores; coreID++ {
		totalConns += len(backend.coreConns[coreID])
	}

	if totalConns == 0 {
		return errors.New("failed to establish any connections to backend")
	}

	// Start backend goroutines
	go backend.batcherLoop()
	go backend.controlLoop()
	go backend.evictionBatcherLoop()

	p.backends[serverID] = backend
	logger.Info("Added backend", "server_id", serverID, "address", address, "cores", nrCores, "connections", totalConns, "per_core", p.connsPerCore)
	return nil
}

// AddStaticBackend adds a backend without discovery.
// Used in local mode to point directly at a backend.
func (p *Pool) AddStaticBackend(serverID, address string) error {
	logger.Debug("Adding static backend", "server_id", serverID, "address", address)
	return p.AddBackend(serverID, address)
}

// RemoveBackend removes a backend server from the pool
func (p *Pool) RemoveBackend(serverID string) {
	p.mu.Lock()
	backend, exists := p.backends[serverID]
	if exists {
		delete(p.backends, serverID)
	}
	p.mu.Unlock()

	if backend != nil {
		backend.Close()
	}
}

// GetBackend returns a backend by server ID
func (p *Pool) GetBackend(serverID string) (*Backend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	backend, exists := p.backends[serverID]
	if !exists {
		return nil, ErrNoConnections
	}

	return backend, nil
}

// GetOrCreateBackend returns an existing backend or creates a new one
func (p *Pool) GetOrCreateBackend(serverID, address string) (*Backend, error) {
	p.mu.RLock()
	backend, exists := p.backends[serverID]
	p.mu.RUnlock()

	if exists {
		return backend, nil
	}

	// Create new backend
	err := p.AddBackend(serverID, address)
	if err != nil {
		return nil, err
	}

	p.mu.RLock()
	backend = p.backends[serverID]
	p.mu.RUnlock()

	return backend, nil
}

// Close closes all backend connections
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	backends := p.backends
	p.backends = make(map[string]*Backend)
	p.mu.Unlock()

	for _, backend := range backends {
		backend.Close()
	}
}


// inboxMessage wraps a message with its target core ID
type inboxMessage struct {
	msg    *Message
	coreID int
}

// Backend represents a single backend server with per-core connections
type Backend struct {
	ServerID string
	Address  string // host:port

	mu          sync.RWMutex
	nrCores     int        // Number of cores on this server
	coreConns   [][]*Conn  // Per-core connection lists: coreConns[coreID] = [conn1, conn2, ...]
	coreNext    []int      // Round-robin index per core
	clientToCore map[uint32]int // Maps client ID to their assigned core

	// Per-core project tracking for targeted CONFIG_PUSH
	coreProjects []map[string]bool      // coreProjects[coreID][projectID] = true
	projectCores map[string]map[int]bool // projectCores[projectID][coreID] = true

	// Per-core metrics for heartbeat aggregation
	coreMetrics     []CoreMetrics
	lastDBHeartbeat time.Time // Last time we wrote heartbeat to DB

	pool *Pool

	// Channels for lock-free communication
	inbox        chan *inboxMessage   // Client messages to send to backend
	controlChan  chan *ControlMessage // Control messages from server (HEARTBEAT, DATABASE_LOADED, etc.)
	evictionChan chan EvictionRequest // Eviction requests to batch

	// Shutdown
	done   chan struct{}
	closed bool
}

// QueueStats returns current channel depths for monitoring
type QueueStats struct {
	InboxLen int
	InboxCap int
}

// GetQueueStats returns queue depths for all backends
func (p *Pool) GetQueueStats() map[string]QueueStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]QueueStats)
	for id, b := range p.backends {
		stats[id] = QueueStats{
			InboxLen: len(b.inbox),
			InboxCap: cap(b.inbox),
		}
	}
	return stats
}

// connectWithHandshake establishes a new connection and performs HELLO handshake
// Returns the connection and the core ID it was assigned to
func (b *Backend) connectWithHandshake() (*Conn, int, error) {
	dialer := net.Dialer{Timeout: b.pool.connectTimeout}
	netConn, err := dialer.Dial("tcp", b.Address)
	if err != nil {
		return nil, 0, err
	}

	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		tcpConn.SetWriteBuffer(2 * 1024 * 1024)
	}

	// Send HELLO
	if err := WriteHello(netConn, ProxyVersion); err != nil {
		netConn.Close()
		return nil, 0, err
	}

	// Read HELLO_ACK
	helloAck, err := ReadHelloAck(netConn)
	if err != nil {
		netConn.Close()
		return nil, 0, err
	}

	coreID := int(helloAck.CoreID)
	conn := NewConn(netConn, b, coreID)
	return conn, coreID, nil
}

// RegisterClient registers a client with its target core based on project/database ID
// Must be called before sending messages for this client
func (b *Backend) RegisterClient(clientID uint32, projectID, databaseID string) int {
	// Hash the full path to match backend's sharding: "project_id/database_id"
	fullPath := projectID + "/" + databaseID
	coreID := CoreForDatabase(fullPath, b.nrCores)

	b.mu.Lock()
	b.clientToCore[clientID] = coreID
	b.mu.Unlock()

	return coreID
}

// UnregisterClient removes a client from the core mapping
func (b *Backend) UnregisterClient(clientID uint32) {
	b.mu.Lock()
	delete(b.clientToCore, clientID)
	b.mu.Unlock()
}

// SendMessage sends a message to the backend (non-blocking via inbox channel)
// The client must be registered first via RegisterClient
func (b *Backend) SendMessage(msg *Message) error {
	b.mu.RLock()
	coreID, ok := b.clientToCore[msg.ClientID]
	b.mu.RUnlock()

	if !ok {
		return ErrClientNotRouted
	}

	select {
	case b.inbox <- &inboxMessage{msg: msg, coreID: coreID}:
		return nil
	default:
		logger.Warn("Inbox full, dropping message", "server_id", b.ServerID, "client_id", msg.ClientID)
		return errors.New("backend inbox full")
	}
}

// SendConnect sends a CONNECT message for a new client
// This also registers the client with the target core
func (b *Backend) SendConnect(clientID uint32, projectID, databaseID string, payload []byte) error {
	coreID := b.RegisterClient(clientID, projectID, databaseID)

	msg := &Message{
		Type:     MsgTypeConnect,
		ClientID: clientID,
		Payload:  payload,
	}

	select {
	case b.inbox <- &inboxMessage{msg: msg, coreID: coreID}:
		return nil
	default:
		logger.Warn("Inbox full, dropping CONNECT", "server_id", b.ServerID, "client_id", clientID)
		return errors.New("backend inbox full")
	}
}

// batcherLoop accumulates messages and flushes them as batches.
//
// HOT PATH: This is the single goroutine that processes all client→backend messages
// for this backend. It batches messages to reduce syscalls (write fewer, larger chunks).
//
// Batching strategy:
//   - Time-based: Flush every 3ms to bound latency
//   - Size-based: Flush at 1MB to prevent unbounded accumulation
//   - Whichever comes first triggers the flush
//
// Why single goroutine? Ensures message ordering within a core. Messages for the
// same client always go through the same batcher → same connection → same core.
//
// The 3ms interval is a tradeoff: lower = better latency, higher = fewer syscalls.
// At 3ms with typical message sizes, we get good batching without noticeable delay.
func (b *Backend) batcherLoop() {
	// Pre-allocate batch slice to avoid repeated allocations
	batch := make([]*inboxMessage, 0, 1000)
	ticker := time.NewTicker(3 * time.Millisecond)
	defer ticker.Stop()

	batchBytes := 0
	const maxBatchBytes = 1024 * 1024 // 1MB - prevents memory spike if inbox fills faster than flush

	for {
		select {
		case msg := <-b.inbox:
			// Accumulate message into batch
			batch = append(batch, msg)
			batchBytes += len(msg.msg.Payload) + 9 // 9 = header size (4 len + 1 type + 4 clientID)

			// Size trigger: flush immediately if batch is large enough
			// This prevents unbounded memory growth during traffic spikes
			if batchBytes >= maxBatchBytes {
				b.flushBatch(batch)
				batch = batch[:0] // Reset slice but keep capacity
				batchBytes = 0
			}

		case <-ticker.C:
			// Time trigger: flush periodically to bound latency
			// Even small batches get sent within 3ms of first message
			if len(batch) > 0 {
				b.flushBatch(batch)
				batch = batch[:0]
				batchBytes = 0
			}

		case <-b.done:
			// Graceful shutdown: flush remaining messages before exit
			if len(batch) > 0 {
				b.flushBatch(batch)
			}
			return
		}
	}
}

// evictionBatcherLoop batches eviction requests and flushes them to the database
func (b *Backend) evictionBatcherLoop() {
	batch := make([]EvictionRequest, 0, 500)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	const maxBatchSize = 500

	for {
		select {
		case req := <-b.evictionChan:
			batch = append(batch, req)

			if len(batch) >= maxBatchSize {
				b.flushEvictions(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.flushEvictions(batch)
				batch = batch[:0]
			}

		case <-b.done:
			if len(batch) > 0 {
				b.flushEvictions(batch)
			}
			return
		}
	}
}

// flushEvictions sends a batch of evictions to the database
func (b *Backend) flushEvictions(batch []EvictionRequest) {
	if len(batch) == 0 || b.pool.configProvider == nil {
		return
	}

	if err := b.pool.configProvider.EvictDatabases(batch); err != nil {
		logger.Error("Failed to evict databases", "server_id", b.ServerID, "count", len(batch), "error", err)
	} else {
		logger.Debug("Evicted databases in batch", "server_id", b.ServerID, "count", len(batch))
	}
}

// flushBatch writes a batch of messages to backend connections.
//
// HOT PATH: Called by batcherLoop every 3ms or when batch reaches 1MB.
// This is where batched messages actually go out over TCP.
//
// Strategy:
//  1. Group messages by destination core (each core handles specific databases)
//  2. Within each core, shard by clientID across connections (for parallelism)
//  3. Write to all connections in parallel using goroutines
//
// Why ClientID sharding? Messages for the same client always go through the same
// connection, ensuring ordering. Different clients can use different connections,
// giving us parallelism without sacrificing per-client ordering.
//
// Connection recovery happens lazily here: if a connection is dead, we reconnect
// before writing. This is simpler than a separate health-check goroutine.
func (b *Backend) flushBatch(batch []*inboxMessage) {
	if len(batch) == 0 {
		return
	}

	b.mu.Lock()

	// Group messages by core
	coreMessages := make(map[int][]*Message)
	for _, im := range batch {
		coreMessages[im.coreID] = append(coreMessages[im.coreID], im.msg)
	}

	// Process each core's messages
	var wg sync.WaitGroup
	for coreID, messages := range coreMessages {
		if coreID >= len(b.coreConns) {
			logger.Error("Invalid core ID", "server_id", b.ServerID, "core_id", coreID, "nr_cores", b.nrCores)
			continue
		}

		conns := b.coreConns[coreID]
		if len(conns) == 0 {
			logger.Warn("No connections for core", "server_id", b.ServerID, "core_id", coreID)
			continue
		}

		// Check for dead connections and reconnect
		for i, conn := range conns {
			if conn == nil || conn.IsClosed() {
				newConn, newCoreID, err := b.connectWithHandshake()
				if err != nil {
					logger.Warn("Reconnect failed for core", "server_id", b.ServerID, "core_id", coreID, "error", err)
					continue
				}
				// Verify we got assigned to the same core
				if newCoreID != coreID {
					// Got assigned to different core, close and retry
					newConn.Close()
					continue
				}
				b.coreConns[coreID][i] = newConn
				go newConn.readLoop()
			}
		}

		// Copy connections for parallel writes
		connsCopy := make([]*Conn, len(b.coreConns[coreID]))
		copy(connsCopy, b.coreConns[coreID])
		numConns := len(connsCopy)

		// Shard messages by ClientID within this core's connections
		shards := make([][]*Message, numConns)
		for _, msg := range messages {
			shard := int(msg.ClientID) % numConns
			shards[shard] = append(shards[shard], msg)
		}

		// Write to each connection in parallel
		for i, shard := range shards {
			if len(shard) == 0 {
				continue
			}

			conn := connsCopy[i]
			if conn == nil {
				continue
			}

			wg.Add(1)
			go func(connIdx int, conn *Conn, msgs []*Message, coreID int) {
				defer wg.Done()

				for _, msg := range msgs {
					if err := conn.WriteMessage(msg); err != nil {
						logger.Error("Write error", "server_id", b.ServerID, "core_id", coreID, "conn_idx", connIdx, "error", err)
						conn.Close()
						return
					}
				}

				if err := conn.Flush(); err != nil {
					logger.Warn("Flush error", "server_id", b.ServerID, "core_id", coreID, "conn_idx", connIdx, "error", err)
				}
			}(i, conn, shard, coreID)
		}
	}

	b.mu.Unlock()
	wg.Wait()
}


// handleConnDeath is called when a connection dies
func (b *Backend) handleConnDeath(conn *Conn) {
	b.mu.Lock()

	coreID := conn.coreID
	if coreID >= 0 && coreID < len(b.coreConns) {
		for i, c := range b.coreConns[coreID] {
			if c == conn {
				b.coreConns[coreID][i] = nil
				break
			}
		}
	}
	logger.Warn("Connection died, will reconnect on next flush", "server_id", b.ServerID, "core_id", coreID)

	// Check if ALL connections to the backend are now dead
	// If so, notify clients immediately rather than waiting for discovery loop
	allDead := true
	for _, coreConns := range b.coreConns {
		for _, c := range coreConns {
			if c != nil && !c.IsClosed() {
				allDead = false
				break
			}
		}
		if !allDead {
			break
		}
	}

	b.mu.Unlock()

	// If all connections are dead, notify proxy to close clients immediately
	// This provides faster detection than waiting for the 15s discovery loop
	if allDead && !b.closed && b.pool.clientNotifier != nil {
		logger.Warn("All connections dead, notifying clients", "server_id", b.ServerID)
		b.pool.clientNotifier.OnBackendDisconnected(b.ServerID)
	}
}

// Close closes the backend and all its connections
func (b *Backend) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	close(b.done)

	// Collect all connections
	var allConns []*Conn
	for _, coreConns := range b.coreConns {
		for _, conn := range coreConns {
			if conn != nil {
				allConns = append(allConns, conn)
			}
		}
	}
	b.coreConns = nil
	b.mu.Unlock()

	for _, conn := range allConns {
		conn.Close()
	}

	// Notify proxy to close all clients that were using this backend
	// This ensures clients reconnect and get routed to a healthy server
	if b.pool.clientNotifier != nil {
		b.pool.clientNotifier.OnBackendDisconnected(b.ServerID)
	}
}

// NrCores returns the number of cores on this backend
func (b *Backend) NrCores() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.nrCores
}

// =============================================================================
// Coordinator Protocol Handlers
// =============================================================================

// controlLoop handles control messages from the server (HEARTBEAT, DATABASE_LOADED, etc.)
func (b *Backend) controlLoop() {
	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case msg := <-b.controlChan:
			b.handleControlMessage(msg)

		case <-heartbeatTicker.C:
			// Periodically write aggregated heartbeat to DB
			b.maybeWriteHeartbeatToDB()

		case <-b.done:
			return
		}
	}
}

// handleControlMessage dispatches a control message to the appropriate handler
func (b *Backend) handleControlMessage(msg *ControlMessage) {
	switch msg.Type {
	case MsgTypeHeartbeat:
		b.handleHeartbeat(msg)
	case MsgTypeDatabaseLoaded:
		b.handleDatabaseLoaded(msg)
	case MsgTypeDatabaseUnloaded:
		b.handleDatabaseUnloaded(msg)
	case MsgTypeConfigRequest:
		b.handleConfigRequest(msg)
	default:
		logger.Warn("Unknown control message type", "server_id", b.ServerID, "type", msg.Type)
	}
}

// handleHeartbeat processes a HEARTBEAT message from a core
func (b *Backend) handleHeartbeat(msg *ControlMessage) {
	hb, err := DecodeHeartbeat(msg.Payload)
	if err != nil {
		logger.Warn("Invalid heartbeat payload", "server_id", b.ServerID, "error", err)
		return
	}

	// Extract coreID from the message context (stored when received)
	// For now, we use the payload to determine which core sent it
	// The coreID is attached by the connection's readLoop
	coreID := 0
	if len(msg.Payload) > 16 {
		// CoreID is appended after the standard payload by readLoop
		coreID = int(msg.Payload[16])
	}

	b.mu.Lock()
	if coreID >= 0 && coreID < len(b.coreMetrics) {
		b.coreMetrics[coreID] = CoreMetrics{
			Load:     hb.Load,
			Clients:  hb.Clients,
			MemMB:    hb.MemMB,
			LastSeen: time.Now(),
		}
	}
	b.mu.Unlock()

	// Send HEARTBEAT_ACK back
	// This needs to go to the specific connection that sent the heartbeat
	// For simplicity, we'll send it on any connection to that core
	b.sendHeartbeatAck(coreID)
}

// sendHeartbeatAck sends a HEARTBEAT_ACK to a specific core
func (b *Backend) sendHeartbeatAck(coreID int) {
	payload := EncodeHeartbeatAck(uint64(time.Now().UnixMilli()))

	b.mu.RLock()
	if coreID >= 0 && coreID < len(b.coreConns) && len(b.coreConns[coreID]) > 0 {
		conn := b.coreConns[coreID][0]
		if conn != nil && !conn.IsClosed() {
			b.mu.RUnlock()
			if err := conn.WriteControlMessage(MsgTypeHeartbeatAck, payload); err != nil {
				logger.Warn("Failed to send HEARTBEAT_ACK", "server_id", b.ServerID, "core_id", coreID, "error", err)
			}
			return
		}
	}
	b.mu.RUnlock()
}

// maybeWriteHeartbeatToDB writes aggregated heartbeat to the database if enough time has passed
func (b *Backend) maybeWriteHeartbeatToDB() {
	b.mu.Lock()
	if time.Since(b.lastDBHeartbeat) < 10*time.Second {
		b.mu.Unlock()
		return
	}
	b.lastDBHeartbeat = time.Now()

	// Aggregate metrics from all cores
	var totalLoad, totalClients, totalMem int
	activeCores := 0
	for _, metrics := range b.coreMetrics {
		if time.Since(metrics.LastSeen) < 30*time.Second {
			totalLoad += int(metrics.Load)
			totalClients += int(metrics.Clients)
			totalMem += int(metrics.MemMB)
			activeCores++
		}
	}
	b.mu.Unlock()

	if activeCores == 0 {
		return // No recent metrics
	}

	avgLoad := totalLoad / activeCores

	// Write to DB via config provider
	if b.pool.configProvider != nil {
		if err := b.pool.configProvider.UpdateServerHeartbeat(b.ServerID, avgLoad, totalClients, totalMem); err != nil {
			logger.Error("Failed to write heartbeat to DB", "server_id", b.ServerID, "error", err)
		} else {
			logger.Debug("Wrote heartbeat to DB", "server_id", b.ServerID, "load", avgLoad, "clients", totalClients, "mem_mb", totalMem)
		}
	}
}

// handleDatabaseLoaded processes a DATABASE_LOADED message from a core
func (b *Backend) handleDatabaseLoaded(msg *ControlMessage) {
	payload, err := DecodeDatabaseLoaded(msg.Payload)
	if err != nil {
		logger.Warn("Invalid DATABASE_LOADED payload", "server_id", b.ServerID, "error", err)
		return
	}

	// Extract coreID (appended by readLoop)
	coreID := 0
	standardLen := 1 + len(payload.ProjectID) + 1 + len(payload.DatabaseID)
	if len(msg.Payload) > standardLen {
		coreID = int(msg.Payload[standardLen])
	}

	b.mu.Lock()
	// Track that this core has this project
	if coreID >= 0 && coreID < len(b.coreProjects) {
		b.coreProjects[coreID][payload.ProjectID] = true
	}

	// Update reverse index
	if b.projectCores[payload.ProjectID] == nil {
		b.projectCores[payload.ProjectID] = make(map[int]bool)
	}
	b.projectCores[payload.ProjectID][coreID] = true
	b.mu.Unlock()

	// Note: We don't track database_count here anymore - it's informational only
	// and can be computed from SELECT COUNT(*) when needed

	logger.Debug("Core loaded database", "server_id", b.ServerID, "core_id", coreID, "project_id", payload.ProjectID, "database_id", payload.DatabaseID)
}

// handleDatabaseUnloaded processes a DATABASE_UNLOADED message from a core
func (b *Backend) handleDatabaseUnloaded(msg *ControlMessage) {
	payload, err := DecodeDatabaseUnloaded(msg.Payload)
	if err != nil {
		logger.Warn("Invalid DATABASE_UNLOADED payload", "server_id", b.ServerID, "error", err)
		return
	}

	// Extract coreID (appended by readLoop)
	coreID := 0
	standardLen := 1 + len(payload.ProjectID) + 1 + len(payload.DatabaseID) + 1 + 1 // +1 for ephemeral flag
	if len(msg.Payload) > standardLen {
		coreID = int(msg.Payload[standardLen])
	}

	b.mu.Lock()
	// Remove from reverse index
	if cores, ok := b.projectCores[payload.ProjectID]; ok {
		delete(cores, coreID)
		if len(cores) == 0 {
			delete(b.projectCores, payload.ProjectID)
		}
	}

	// Remove from core's project set
	if coreID >= 0 && coreID < len(b.coreProjects) {
		delete(b.coreProjects[coreID], payload.ProjectID)
	}
	b.mu.Unlock()

	// Queue eviction for batching (non-blocking)
	select {
	case b.evictionChan <- EvictionRequest{ProjectID: payload.ProjectID, DatabaseID: payload.DatabaseID}:
	default:
		logger.Warn("Eviction channel full, dropping eviction", "server_id", b.ServerID, "project_id", payload.ProjectID, "database_id", payload.DatabaseID)
	}

	// Notify proxy to close clients connected to this database
	// This ensures clients reconnect and get routed to a new server
	if b.pool.clientNotifier != nil {
		b.pool.clientNotifier.OnDatabaseUnloaded(b.ServerID, payload.ProjectID, payload.DatabaseID)
	}

	logger.Debug("Core unloaded database", "server_id", b.ServerID, "core_id", coreID, "project_id", payload.ProjectID, "database_id", payload.DatabaseID, "reason", payload.Reason, "ephemeral", payload.Ephemeral)
}

// handleConfigRequest processes a CONFIG_REQUEST message from a core
func (b *Backend) handleConfigRequest(msg *ControlMessage) {
	payload, err := DecodeConfigRequest(msg.Payload)
	if err != nil {
		logger.Warn("Invalid CONFIG_REQUEST payload", "server_id", b.ServerID, "error", err)
		return
	}

	// Extract coreID (appended by readLoop)
	coreID := 0
	standardLen := 1 + len(payload.ProjectID)
	if len(msg.Payload) > standardLen {
		coreID = int(msg.Payload[standardLen])
	}

	logger.Debug("Core requested config", "server_id", b.ServerID, "core_id", coreID, "project_id", payload.ProjectID)

	// Fetch config from provider
	if b.pool.configProvider == nil {
		logger.Error("No config provider set, cannot respond to CONFIG_REQUEST", "server_id", b.ServerID)
		return
	}

	config, err := b.pool.configProvider.GetProjectConfig(payload.ProjectID)
	if err != nil {
		logger.Error("Failed to get project config", "server_id", b.ServerID, "project_id", payload.ProjectID, "error", err)
		return
	}

	// Send CONFIG_PUSH to the requesting core
	if err := b.SendConfigPushToCore(coreID, payload.ProjectID, config); err != nil {
		logger.Warn("Failed to send CONFIG_PUSH to core", "server_id", b.ServerID, "core_id", coreID, "error", err)
	}
}

// =============================================================================
// Coordinator Protocol Senders
// =============================================================================

// SendConfigPushToCore sends a CONFIG_PUSH to a specific core
func (b *Backend) SendConfigPushToCore(coreID int, projectID string, config *ProjectConfig) error {
	payload, err := EncodeConfigPush(projectID, config)
	if err != nil {
		return err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if coreID < 0 || coreID >= len(b.coreConns) {
		return ErrCoreUnavailable
	}

	// Send on first available connection to this core
	for _, conn := range b.coreConns[coreID] {
		if conn != nil && !conn.IsClosed() {
			return conn.WriteControlMessage(MsgTypeConfigPush, payload)
		}
	}

	return ErrCoreUnavailable
}

// SendConfigPushToProject sends a CONFIG_PUSH to all cores that have the given project loaded
func (b *Backend) SendConfigPushToProject(projectID string, config *ProjectConfig) error {
	payload, err := EncodeConfigPush(projectID, config)
	if err != nil {
		return err
	}

	b.mu.RLock()
	cores := make([]int, 0)
	if coreset, ok := b.projectCores[projectID]; ok {
		for coreID := range coreset {
			cores = append(cores, coreID)
		}
	}
	b.mu.RUnlock()

	var lastErr error
	for _, coreID := range cores {
		if err := b.sendControlToCore(coreID, MsgTypeConfigPush, payload); err != nil {
			lastErr = err
			logger.Warn("Failed to send CONFIG_PUSH to core", "server_id", b.ServerID, "core_id", coreID, "error", err)
		}
	}

	return lastErr
}

// SendEvictDatabase sends an EVICT_DATABASE message to the core that owns the database.
// If purge is true, the backend also deletes any on-disk data for persistent databases.
func (b *Backend) SendEvictDatabase(projectID, databaseID string, purge bool) error {
	// Hash the full path to match backend's sharding: "project_id/database_id"
	fullPath := projectID + "/" + databaseID
	coreID := CoreForDatabase(fullPath, b.nrCores)
	payload := EncodeEvictDatabase(projectID, databaseID, purge)
	return b.sendControlToCore(coreID, MsgTypeEvictDatabase, payload)
}

// SendShutdown sends a SHUTDOWN message to all cores
func (b *Backend) SendShutdown(gracePeriodSec uint32) error {
	payload := EncodeShutdown(gracePeriodSec)

	b.mu.RLock()
	defer b.mu.RUnlock()

	var lastErr error
	for coreID := range b.coreConns {
		for _, conn := range b.coreConns[coreID] {
			if conn != nil && !conn.IsClosed() {
				if err := conn.WriteControlMessage(MsgTypeShutdown, payload); err != nil {
					lastErr = err
				}
			}
		}
	}

	return lastErr
}

// sendControlToCore sends a control message to a specific core
func (b *Backend) sendControlToCore(coreID int, msgType byte, payload []byte) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if coreID < 0 || coreID >= len(b.coreConns) {
		return ErrCoreUnavailable
	}

	for _, conn := range b.coreConns[coreID] {
		if conn != nil && !conn.IsClosed() {
			return conn.WriteControlMessage(msgType, payload)
		}
	}

	return ErrCoreUnavailable
}

// EnqueueControlMessage adds a control message to the control channel for processing
// Called by Conn.readLoop when it receives a control message from the server
func (b *Backend) EnqueueControlMessage(coreID int, msg *ControlMessage) {
	// Append coreID to payload so handlers know which core sent it
	extendedPayload := make([]byte, len(msg.Payload)+1)
	copy(extendedPayload, msg.Payload)
	extendedPayload[len(msg.Payload)] = byte(coreID)
	msg.Payload = extendedPayload

	select {
	case b.controlChan <- msg:
	default:
		logger.Warn("Control channel full, dropping message", "server_id", b.ServerID, "type", msg.Type)
	}
}

// PushConfigToAllBackends pushes a config update to all backends that have the project loaded
func (p *Pool) PushConfigToAllBackends(projectID string, config *ProjectConfig) {
	p.mu.RLock()
	backends := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		backends = append(backends, b)
	}
	p.mu.RUnlock()

	for _, b := range backends {
		if err := b.SendConfigPushToProject(projectID, config); err != nil {
			logger.Warn("Failed to push config to backend", "project_id", projectID, "server_id", b.ServerID, "error", err)
		}
	}
}

// EvictDatabaseOnAllBackends sends EVICT_DATABASE to every connected backend.
// Used when the admin caller doesn't know which backend owns the database (e.g.
// on delete, where the Postgres row is gone by the time the NOTIFY is received).
// EVICT is idempotent on the backend — backends that never had the database
// simply ignore the message. Returns the number of backends that accepted the
// send (not necessarily the number that had the database loaded).
func (p *Pool) EvictDatabaseOnAllBackends(projectID, databaseID string, purge bool) int {
	p.mu.RLock()
	backends := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		backends = append(backends, b)
	}
	p.mu.RUnlock()

	sent := 0
	for _, b := range backends {
		if err := b.SendEvictDatabase(projectID, databaseID, purge); err != nil {
			logger.Warn("Failed to evict database on backend", "project_id", projectID, "database_id", databaseID, "server_id", b.ServerID, "purge", purge, "error", err)
			continue
		}
		sent++
	}
	return sent
}

// =============================================================================
// Server Discovery
// =============================================================================

// ServerInfo contains the information needed to connect to a server
type ServerInfo struct {
	ServerID string
	Address  string // private_ip:port
}

// ServerDiscovery is the interface for discovering servers from the database
type ServerDiscovery interface {
	// GetServersForDiscovery returns all servers that the proxy should connect to
	GetServersForDiscovery() ([]ServerInfo, error)
}

// StartDiscoveryLoop starts a background goroutine that periodically discovers
// new servers from the database and connects to them
func (p *Pool) StartDiscoveryLoop(discovery ServerDiscovery, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}

	go func() {
		// Run immediately on startup
		p.discoverServers(discovery)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.discoverServers(discovery)
			}
		}
	}()

	logger.Info("Server discovery loop started", "interval", interval)
}

// discoverServers queries the database for servers and connects to any new ones
// Also removes unhealthy backends so they can be reconnected
func (p *Pool) discoverServers(discovery ServerDiscovery) {
	servers, err := discovery.GetServersForDiscovery()
	if err != nil {
		logger.Error("Discovery error", "error", err)
		return
	}

	for _, s := range servers {
		p.mu.RLock()
		existing, exists := p.backends[s.ServerID]
		closed := p.closed
		p.mu.RUnlock()

		if closed {
			return
		}

		if exists {
			// Check if existing backend is healthy
			if !existing.hasHealthyConnections() {
				logger.Warn("Backend has no healthy connections, removing for reconnect", "server_id", s.ServerID)
				p.RemoveBackend(s.ServerID)
				exists = false
			}
		}

		if !exists {
			logger.Info("Discovered new server", "server_id", s.ServerID, "address", s.Address)
			if err := p.AddBackend(s.ServerID, s.Address); err != nil {
				logger.Warn("Failed to connect to discovered server", "server_id", s.ServerID, "error", err)
			}
		}
	}
}

// HasBackend returns true if the pool has a backend with the given server ID
func (p *Pool) HasBackend(serverID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.backends[serverID]
	return exists
}

// GetHealthyBackendIDs returns the IDs of all backends with healthy connections
func (p *Pool) GetHealthyBackendIDs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var ids []string
	for id, backend := range p.backends {
		if backend.hasHealthyConnections() {
			ids = append(ids, id)
		}
	}
	return ids
}

// hasHealthyConnections returns true if the backend has at least one healthy connection
func (b *Backend) hasHealthyConnections() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, conns := range b.coreConns {
		for _, conn := range conns {
			if conn != nil && !conn.IsClosed() {
				return true
			}
		}
	}
	return false
}

// PickHealthyBackend returns a healthy backend for new database assignments
// Uses a simple strategy: pick the backend with the fewest clients
func (p *Pool) PickHealthyBackend() *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var best *Backend
	var bestClients uint32 = ^uint32(0) // max uint32

	for _, backend := range p.backends {
		if !backend.hasHealthyConnections() {
			continue
		}

		// Sum clients across all cores
		backend.mu.RLock()
		var totalClients uint32
		for _, metrics := range backend.coreMetrics {
			if time.Since(metrics.LastSeen) < 30*time.Second {
				totalClients += metrics.Clients
			}
		}
		backend.mu.RUnlock()

		if best == nil || totalClients < bestClients {
			best = backend
			bestClients = totalClients
		}
	}

	return best
}
