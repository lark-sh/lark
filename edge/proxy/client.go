// Package proxy handles client connections and proxies them to backend servers.
//
// # Architecture Overview
//
// The proxy is the client-facing component of the Lark system. It:
//   - Terminates TLS and handles WebSocket/WebTransport/HTTP connections
//   - Parses client protocol messages (Lark native or Firebase-compatible)
//   - Routes clients to appropriate backend servers
//   - Multiplexes many client connections over fewer backend connections
//
// # Client Lifecycle
//
// 1. Connection: Client connects via WebSocket, WebTransport, REST, SSE, or Long Polling
// 2. Transport: Connection is wrapped in a ClientTransport interface
// 3. ClientConn: Created to manage state, routing, and message flow
// 4. Routing: First message identifies project/database; proxy looks up backend
// 5. Forwarding: Messages flow bidirectionally between client and backend
// 6. Closure: On disconnect, client is unregistered and backend notified
//
// # State Machine
//
//	CONNECTED ──[join/path msg]──→ ROUTING ──[backend assigned]──→ FORWARDING
//	     │                            │                                │
//	     └──[error]──→ CLOSING ←──────┴────────────────────────────────┘
//
// StateConnected: Just connected, waiting for first message with routing info
// StateRouting: Looking up backend, buffering incoming messages
// StateForwarding: Actively relaying messages to/from backend
// StateClosing: Shutting down, cleaning up resources
//
// # Protocol Support
//
// The proxy supports two client protocols:
//
// Lark Protocol (ProtocolLark):
//   - Modern JSON-based protocol
//   - Messages have "o" (operation) field: j (join), a (auth), d (data), etc.
//   - Used by Lark SDK
//
// Firebase Protocol (ProtocolFirebase):
//   - Firebase RTDB compatible protocol
//   - Nested message structure: {"t":"d","d":{"r":1,"a":"p","b":{...}}}
//   - Used by Firebase SDK for compatibility
//
// # Transport Abstraction
//
// All transports implement ClientTransport interface:
//   - WebSocketTransport: Standard WebSocket connections
//   - WebTransportTransport: QUIC-based WebTransport (with datagrams)
//   - LongPollTransport: Firebase-compatible long polling
//   - RESTTransport: Request-response for REST API
//   - SSETransport: Server-sent events for streaming
//
// # Message Flow
//
// Client → Backend:
//  1. Transport calls client.OnMessage(data)
//  2. Client parses protocol, extracts routing info if needed
//  3. Message forwarded via backend.SendMessage()
//
// Backend → Client:
//  1. Backend calls client.Deliver(data)
//  2. Message queued in client's outbox channel (non-blocking)
//  3. writeLoop sends to transport
//
// # Concurrency Model
//
// Each ClientConn has:
//   - One read goroutine (transport-specific, calls OnMessage)
//   - One write goroutine (writeLoop, drains outbox)
//   - Potentially async routing/auth goroutines
//
// The outbox channel (1000 messages) provides backpressure. If full, messages
// are dropped and the client should reconnect.
//
// # Authentication
//
// Auth tokens can arrive before or after routing completes:
//   - Token extracted from protocol messages
//   - Validated via auth.MultiValidator
//   - Claims forwarded to backend in AUTH message
//   - Backend enforces security rules using these claims
package proxy

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"

	"github.com/lark-sh/lark/edge/auth"
	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
	"github.com/lark-sh/lark/edge/rules"
)

// ClientState represents the connection state
type ClientState int

const (
	StateConnected  ClientState = iota // Just connected, waiting for routing info
	StateRouting                       // Looking up backend
	StateForwarding                    // Relay mode
	StateClosing                       // Shutting down
)

// Protocol type
type Protocol int

const (
	ProtocolLark     Protocol = iota // Modern Lark client
	ProtocolFirebase                 // Legacy Firebase client
)

// outboxMessage is a message queued for sending to the client
type outboxMessage struct {
	data     []byte
	reliable bool
}

const clientOutboxSize = 1000 // Max queued messages per client (~50s at 20hz, handles backend bursts)

// ClientConn represents a client connection being proxied
type ClientConn struct {
	id       uint32
	server   *Server
	protocol Protocol
	state    atomic.Int32 // ClientState

	// Transport-specific connection
	transport ClientTransport

	// Outbox for messages to send to client (lock-free)
	outbox chan *outboxMessage
	done   chan struct{}

	// Project config (fetched at connection time, needed for auth and JoinAck)
	projectID     string
	projectConfig *db.Project   // Fetched project config
	projectReady  chan struct{} // Closed when projectConfig is ready
	projectErr    error         // Error from fetching project config

	// Connection info (for metadata in CONNECT)
	hostname string // Original request hostname (for Firebase)

	// Routing info (set after routing complete)
	databaseID string
	backend    *backend.Backend

	// Auth state
	authToken        string        // Pending token (extracted before routing complete)
	authInfo         *auth.Info    // Validated auth info (after routing complete)
	authRequestID    string        // Request ID for pending auth (to send response)
	authResponseSent bool          // True if we've already sent auth response (avoid double-send)
	authProcessing   bool          // True if auth is being processed async
	authDone         chan struct{} // Closed when auth processing completes

	// Lark protocol state
	joinRequestID string // Request ID from join message (to send JoinAck)

	// Message buffer (before routing is complete)
	bufferMu sync.Mutex
	buffer   [][]byte

	// Firebase state
	firebaseState *FirebaseState

	// Shutdown
	closeMu sync.Mutex
	closed  bool
}

// ClientTransport is the interface for transport-specific operations
type ClientTransport interface {
	// Send sends a message to the client
	Send(data []byte, reliable bool) error
	// Close closes the connection
	Close() error
	// TransportType returns the transport protocol type (for wire protocol)
	TransportType() byte
}

// FirebaseState tracks Firebase protocol state
type FirebaseState struct {
	SessionID     string
	AuthToken     string
	Authenticated bool
}

// SwapTransport replaces the client's transport with a new one.
// Used for LP→WS upgrade where the same client switches transports.
// The old transport is closed, and the new transport takes over.
func (c *ClientConn) SwapTransport(newTransport ClientTransport) {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	// Close old transport (but don't close the client)
	if c.transport != nil {
		c.transport.Close()
	}

	// Swap to new transport
	c.transport = newTransport
}

// connectionIDCounter is used to generate unique connection IDs
var connectionIDCounter atomic.Int64

// generateConnectionID creates a unique connection ID for write deduplication
// Format is similar to Firebase push IDs: timestamp-based + counter
func generateConnectionID() string {
	// Use timestamp + counter for uniqueness
	now := time.Now().UnixMilli()
	counter := connectionIDCounter.Add(1)

	// Encode as base62-like string
	const chars = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
	var result [20]byte

	// First 8 chars: timestamp (high bits first)
	for i := 7; i >= 0; i-- {
		result[i] = chars[now%64]
		now /= 64
	}

	// Last 12 chars: counter + randomness
	for i := 19; i >= 8; i-- {
		result[i] = chars[counter%64]
		counter /= 64
	}

	return string(result[:])
}

// newClientConn creates a new client connection
func newClientConn(server *Server, id uint32, transport ClientTransport, protocol Protocol) *ClientConn {
	c := &ClientConn{
		id:           id,
		server:       server,
		transport:    transport,
		protocol:     protocol,
		outbox:       make(chan *outboxMessage, clientOutboxSize),
		done:         make(chan struct{}),
		projectReady: make(chan struct{}),
	}
	c.state.Store(int32(StateConnected))

	if protocol == ProtocolFirebase {
		c.firebaseState = &FirebaseState{}
	}

	protoName := "Lark"
	if protocol == ProtocolFirebase {
		protoName = "Firebase"
	}
	logger.Debug("Client connected", "client_id", id, "protocol", protoName)

	return c
}

// fetchProjectConfig fetches the project config from the database asynchronously.
// Should be called as soon as project ID is known.
func (c *ClientConn) fetchProjectConfig() {
	go func() {
		defer close(c.projectReady)

		if c.server == nil {
			c.projectErr = errors.New("no server")
			return
		}

		project, err := c.server.GetProjectCached(c.server.ctx, c.projectID)
		if err != nil {
			c.projectErr = err
			return
		}
		c.projectConfig = project
	}()
}

// waitForProjectConfig blocks until project config is ready or returns error
func (c *ClientConn) waitForProjectConfig() (*db.Project, error) {
	<-c.projectReady
	return c.projectConfig, c.projectErr
}

// Start starts the client's write goroutine
func (c *ClientConn) Start() {
	go c.writeLoop()
}

// ID returns the client ID
func (c *ClientConn) ID() uint32 {
	return c.id
}

// State returns the current state
func (c *ClientConn) State() ClientState {
	return ClientState(c.state.Load())
}

// SetState sets the state
func (c *ClientConn) SetState(state ClientState) {
	c.state.Store(int32(state))
}

// writeLoop drains the outbox and sends to transport
func (c *ClientConn) writeLoop() {
	for {
		select {
		case msg := <-c.outbox:
			if err := c.transport.Send(msg.data, msg.reliable); err != nil {
				// Write failed (client disconnected or timeout) - close silently
				c.Close()
				return
			}
		case <-c.done:
			return
		}
	}
}

// Deliver delivers a message to the client's outbox (non-blocking)
// Returns false if the outbox is full
func (c *ClientConn) Deliver(data []byte, reliable bool) bool {
	select {
	case c.outbox <- &outboxMessage{data: data, reliable: reliable}:
		return true
	default:
		return false
	}
}

// sendDirect sends a message directly (bypassing outbox, for protocol messages)
// Used for Firebase hello, error messages, etc. before writeLoop is running
func (c *ClientConn) sendDirect(data []byte, reliable bool) {
	c.transport.Send(data, reliable)
}

// OnMessage handles an incoming message from the client
// Called from the transport's read goroutine
func (c *ClientConn) OnMessage(data []byte, reliable bool) {
	state := c.State()

	switch state {
	case StateConnected:
		c.handleConnectedMessage(data)

	case StateRouting:
		// Buffer messages while routing
		c.bufferMu.Lock()
		c.buffer = append(c.buffer, data)
		c.bufferMu.Unlock()

	case StateForwarding:
		// Forward ALL messages to backend
		c.forwardToBackend(data)

		// Additionally handle auth messages locally (to send response to client)
		if c.isAuthMessage(data) {
			c.handleLateAuth(data)
		}

	case StateClosing:
		// Ignore
	}
}

// handleConnectedMessage handles the first message(s) to determine routing
func (c *ClientConn) handleConnectedMessage(data []byte) {
	if c.protocol == ProtocolLark {
		c.handleLarkMessage(data)
	} else {
		c.handleFirebaseMessage(data)
	}
}

// handleLarkMessage handles a modern Lark protocol message
func (c *ClientConn) handleLarkMessage(data []byte) {
	// Always buffer the message - backend needs all messages
	c.bufferMu.Lock()
	c.buffer = append(c.buffer, data)
	c.bufferMu.Unlock()

	// Parse JSON to peek at message type
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		logger.Warn("Invalid JSON from client", "client_id", c.id, "error", err)
		c.Close()
		return
	}

	op, _ := msg["o"].(string)

	switch op {
	case "j":
		// JOIN message - extract database ID, send JoinAck, but DON'T route yet
		// We wait for AUTH before routing so CONNECT includes auth info
		c.handleLarkJoinMessage(msg)

	case "au":
		// AUTH message - validate and send AuthAck
		c.handleLarkAuthMessage(msg, data)

	default:
		// Any other message - this is a "real" message, start routing now
		if c.databaseID == "" {
			return
		}
		if c.State() == StateConnected {
			c.startRouting()
		}
	}
}

// handleLarkJoinMessage processes a JOIN message - extracts database ID, sends JoinAck
// Does NOT start routing - we wait for auth first
func (c *ClientConn) handleLarkJoinMessage(msg map[string]interface{}) {
	path, _ := msg["d"].(string)
	if path == "" {
		c.sendError("missing path in join")
		return
	}

	// Extract request ID for JoinAck
	if reqID, ok := msg["r"].(string); ok {
		c.joinRequestID = reqID
	}

	// Parse "project/database" format to extract database ID
	slashIdx := -1
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			slashIdx = i
			break
		}
	}

	if slashIdx == -1 {
		c.sendError("invalid path format, expected project/database")
		return
	}

	joinProjectID := path[:slashIdx]
	c.databaseID = path[slashIdx+1:]

	logger.Debug("Client join", "client_id", c.id, "project", joinProjectID, "database", c.databaseID)

	// Verify project ID matches subdomain
	if joinProjectID != c.projectID {
		logger.Debug("Project ID mismatch", "client_id", c.id, "subdomain", c.projectID, "join", joinProjectID)
	}

	if c.databaseID == "" {
		c.sendError("missing database in join path")
		return
	}

	// Send JoinAck (async - waits for project config for volatile paths)
	go c.sendLarkJoinAckAsync()
}

// handleLarkAuthMessage processes an AUTH message - validates and sends AuthAck
func (c *ClientConn) handleLarkAuthMessage(msg map[string]interface{}, data []byte) {
	// Extract token and request ID
	if token, ok := msg["t"].(string); ok {
		c.authToken = token
	}
	if reqID, ok := msg["r"].(string); ok {
		c.authRequestID = reqID
	}

	// Mark auth as processing - routing will wait for this
	c.authProcessing = true
	c.authDone = make(chan struct{})

	// Validate and send response (async - waits for project config)
	go c.handleAuthMessage(data)
}

// handleFirebaseMessage handles Firebase protocol messages
func (c *ClientConn) handleFirebaseMessage(data []byte) {
	// Handle keepalive locally (connection-level, not forwarded)
	if string(data) == "0" {
		c.sendDirect([]byte("0"), true)
		return
	}

	// Buffer ALL messages - proxy is transparent, but we forward everything
	c.bufferMu.Lock()
	c.buffer = append(c.buffer, data)
	c.bufferMu.Unlock()

	// Parse JSON to peek for routing info and handle protocol flow
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		logger.Warn("Invalid Firebase JSON from client", "client_id", c.id, "error", err)
		return
	}

	// Check message type
	t, _ := msg["t"].(string)
	d, _ := msg["d"].(map[string]interface{})

	// Handle control messages (type "c")
	if t == "c" && d != nil {
		ct, _ := d["t"].(string)
		switch ct {
		case "a":
			// SWITCH_ACK - client acknowledges switch from LP to WS
			// We don't need to do anything special here, just log it
			logger.Debug("Received SWITCH_ACK", "client_id", c.id)
			// Remove from buffer - this is a local control message, don't forward
			c.bufferMu.Lock()
			if len(c.buffer) > 0 {
				c.buffer = c.buffer[:len(c.buffer)-1]
			}
			c.bufferMu.Unlock()
			return

		case "n":
			// END_TRANSMISSION - LP side of LP→WS upgrade is complete
			// The client is telling us to close this LP connection
			// We should send END_TRANSMISSION back and close
			logger.Debug("Received END_TRANSMISSION", "client_id", c.id)

			// Remove from buffer - this is a local control message, don't forward
			c.bufferMu.Lock()
			if len(c.buffer) > 0 {
				c.buffer = c.buffer[:len(c.buffer)-1]
			}
			c.bufferMu.Unlock()

			// Send END_TRANSMISSION response
			endTx := map[string]interface{}{
				"t": "c",
				"d": map[string]interface{}{
					"t": "n",
					"d": map[string]interface{}{},
				},
			}
			endTxData, _ := sonic.Marshal(endTx)
			c.sendDirect(endTxData, true)

			// Note: We don't close the client here because if this is the LP side
			// of an LP→WS upgrade, the WS side has already taken over.
			// The transport will be closed naturally when the LP session cleans up.
			return
		}
	}

	if t == "d" && d != nil {
		// Data message
		a, _ := d["a"].(string)
		r, _ := d["r"].(float64) // Request ID

		switch a {
		case "auth", "gauth":
			// Auth message - extract token from "cred" field
			// Firebase auth format: {"t":"d","d":{"r":1,"a":"auth","b":{"cred":"<token>"}}}
			if b, ok := d["b"].(map[string]interface{}); ok {
				if cred, ok := b["cred"].(string); ok && cred != "" {
					c.authToken = cred
				}
			}
			// Mark auth as processing - routing will wait for this
			c.authProcessing = true
			c.authDone = make(chan struct{})
			// Validate and send response (async - waits for project config)
			go c.handleAuthMessage(data)

		case "s":
			// Stats message - acknowledge locally
			c.sendFirebaseOK(int(r), "")

		case "q", "n", "p", "m", "g", "o", "om", "oc":
			// Path operation - extract database and start routing
			if b, ok := d["b"].(map[string]interface{}); ok {
				if path, ok := b["p"].(string); ok {
					c.extractDatabaseFromPath(path)
					if c.databaseID != "" {
						// We have routing info - start routing
						// All buffered messages (including this one) will be forwarded
						c.startRouting()
						return
					}
				}
			}
		}
	}
}

// sendFirebaseOK sends a Firebase protocol "ok" response
func (c *ClientConn) sendFirebaseOK(requestID int, data interface{}) {
	response := map[string]interface{}{
		"t": "d",
		"d": map[string]interface{}{
			"r": requestID,
			"b": map[string]interface{}{
				"s": "ok",
				"d": data,
			},
		},
	}
	responseData, _ := sonic.Marshal(response)
	c.sendDirect(responseData, true)
}

// resolveDatabase sets the database for this connection. It's the project's
// "default" database unless one was already selected from the `db--project`
// subdomain convention. (`path` is unused — kept for the call site's binding.)
func (c *ClientConn) extractDatabaseFromPath(path string) {
	_ = path
	// Already set from the `db--project` subdomain convention.
	if c.databaseID != "" {
		return
	}
	c.databaseID = "default"
}

// startRouting initiates the routing lookup for Firebase clients
// (Lark clients use handleLarkJoin which calls startRoutingWithProject directly)
func (c *ClientConn) startRouting() {
	// Safety check for tests where server may be nil
	if c.server == nil {
		c.SetState(StateRouting)
		return
	}

	c.SetState(StateRouting)

	go func() {
		// Wait for any pending auth to complete first
		if c.authProcessing {
			<-c.authDone
		}

		// Wait for project config (should already be ready or nearly ready)
		project, err := c.waitForProjectConfig()
		if err != nil {
			c.sendError("project not found")
			c.Close()
			return
		}

		// Store project config
		c.projectConfig = project

		// Build auth payload from current state
		// If auth was processed, c.authInfo will be set
		// If no auth, use anonymous
		var authPayload *backend.AuthPayload
		if c.authInfo != nil {
			authPayload = &backend.AuthPayload{
				UID:         c.authInfo.UID,
				Provider:    c.authInfo.Provider,
				Claims:      c.authInfo.Token,
				IsTrueAdmin: c.authInfo.IsTrueAdmin,
			}
		} else {
			authPayload = &backend.AuthPayload{
				UID:      "",
				Provider: "anonymous",
				Claims:   make(map[string]any),
			}
		}

		// Continue with routing
		c.startRoutingWithProject(project, authPayload)
	}()
}

// startRoutingWithProject continues routing with an already-fetched project config
func (c *ClientConn) startRoutingWithProject(project *db.Project, authPayload *backend.AuthPayload) {
	// Safety check for tests where server may be nil
	if c.server == nil {
		c.SetState(StateForwarding)
		return
	}

	c.SetState(StateRouting)
	logger.Debug("Routing started", "client_id", c.id, "project", c.projectID, "database", c.databaseID)

	// Get routing data (server assignment for this database)
	server, _, err := c.server.db.EnsureRoutingData(
		c.server.ctx,
		c.projectID,
		c.databaseID,
		int64(c.server.config.HeartbeatTimeout),
	)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			logger.Warn("Routing failed", "client_id", c.id, "project", c.projectID, "database", c.databaseID, "reason", "database_not_found")
			c.sendError("database not found")
		} else if errors.Is(err, db.ErrNoServersAvailable) {
			logger.Warn("Routing failed", "client_id", c.id, "project", c.projectID, "database", c.databaseID, "reason", "no_servers")
			c.sendError("no servers available")
		} else if errors.Is(err, db.ErrInvalidDatabaseID) {
			logger.Warn("Routing failed", "client_id", c.id, "project", c.projectID, "database", c.databaseID, "reason", "invalid_database_id", "error", err)
			c.sendError(err.Error())
		} else {
			logger.Error("Routing error", "client_id", c.id, "project", c.projectID, "database", c.databaseID, "error", err)
			c.sendError("routing error")
		}
		c.Close()
		return
	}

	logger.Debug("Client routed", "client_id", c.id, "project", c.projectID, "database", c.databaseID, "server", server.ID)

	// Get or create backend connection
	// Use private IP for backend connections (internal network)
	// Fall back to public IP if private IP not set
	backendIP := server.PrivateIP
	if backendIP == "" {
		backendIP = server.IPAddress
	}
	backendAddr := backendIP + ":2727" // TODO: Make port configurable
	be, err := c.server.pool.GetOrCreateBackend(server.ID, backendAddr)
	if err != nil {
		logger.Error("Backend connection error", "client_id", c.id, "backend", backendAddr, "error", err)
		c.sendError("backend connection error")
		c.Close()
		return
	}

	c.backend = be

	// Encode auth payload
	authBytes, err := backend.EncodeAuthPayload(authPayload)
	if err != nil {
		logger.Error("Auth encoding error", "client_id", c.id, "error", err)
		authBytes = nil
	}

	// Build metadata for CONNECT
	// For Firebase connections, include firebase: true and hostname
	var metadataBytes []byte
	if c.protocol == ProtocolFirebase {
		metadata := map[string]interface{}{
			"firebase": true,
			"hostname": c.hostname,
		}
		metadataBytes, _ = sonic.Marshal(metadata)
	}

	// Send CONNECT to backend with auth info
	// This also registers the client with the correct core based on database ID
	connectPayload, err := backend.EncodeConnectPayload(&backend.ConnectPayload{
		Protocol:   c.transport.TransportType(),
		ProjectID:  c.projectID,
		DatabaseID: c.databaseID,
		Metadata:   metadataBytes,
		Auth:       authBytes,
	})
	if err != nil {
		logger.Error("Failed to encode CONNECT", "client_id", c.id, "error", err)
		c.sendError("invalid project or database ID")
		c.Close()
		return
	}

	err = be.SendConnect(c.id, c.projectID, c.databaseID, connectPayload)
	if err != nil {
		logger.Error("Failed to send CONNECT", "client_id", c.id, "server", be.ServerID, "error", err)
		c.sendError("backend error")
		c.Close()
		return
	}

	// Log with the full path used for sharding (must match backend)
	fullPath := c.projectID + "/" + c.databaseID
	coreID := backend.CoreForDatabase(fullPath, be.NrCores())
	logger.Debug("Sent CONNECT to backend", "client_id", c.id, "server", be.ServerID, "core", coreID, "database", c.databaseID)

	// Flush buffered messages (including join/auth - backend needs them, just won't send responses)
	c.bufferMu.Lock()
	buffered := c.buffer
	c.buffer = nil
	c.bufferMu.Unlock()

	if len(buffered) > 0 {
		logger.Debug("Flushing buffered messages", "client_id", c.id, "count", len(buffered))
	}

	for _, msg := range buffered {
		// Forward to backend
		be.SendMessage(&backend.Message{
			Type:     backend.MsgTypeData,
			ClientID: c.id,
			Payload:  msg,
		})
	}

	// Now in forwarding mode
	c.SetState(StateForwarding)
	logger.Debug("Client now forwarding", "client_id", c.id, "server", be.ServerID)
}

// validateAuth validates the client's auth token and returns an AuthPayload
// If no token, returns anonymous auth with nil error
// If token provided but validation fails, returns nil payload with error
func (c *ClientConn) validateAuth(project *db.Project) (*backend.AuthPayload, error) {
	// If no token, return anonymous
	if c.authToken == "" {
		return &backend.AuthPayload{
			UID:      "",
			Provider: "anonymous",
			Claims:   make(map[string]any),
		}, nil
	}

	// Validate token using project config
	info, err := c.server.authValidator.ValidateForProject(
		c.authToken,
		project.SecretKey,
		project.AdminSecretKey,
		project.FirebaseProjectID,
	)
	if err != nil {
		return nil, err
	}

	// Store validated auth info
	c.authInfo = info

	// Return auth payload for backend
	return &backend.AuthPayload{
		UID:         info.UID,
		Provider:    info.Provider,
		Claims:      info.Token,
		IsTrueAdmin: info.IsTrueAdmin,
	}, nil
}

// sendLarkJoinAckAsync waits for project config and sends JoinAck
func (c *ClientConn) sendLarkJoinAckAsync() {
	// Wait for project config (needed for volatile paths)
	project, err := c.waitForProjectConfig()
	if err != nil {
		logger.Error("JoinAck project config error", "client_id", c.id, "project", c.projectID, "error", err)
		c.sendError("project not found")
		c.Close()
		return
	}

	// Store project config for auth validation
	c.projectConfig = project

	// Send JoinAck with volatile paths
	c.sendLarkJoinAck(project)
}

// sendLarkJoinAck sends the Lark JoinAck response from the proxy
// Format: {"jc": "requestId", "vp": ["volatile/paths"], "cid": "connectionId", "st": serverTimeMs}
func (c *ClientConn) sendLarkJoinAck(project *db.Project) {
	// Extract volatile paths from rules
	volatilePaths := rules.GetVolatilePaths(project.RulesJSON)

	// Generate connection ID for write deduplication
	connectionID := generateConnectionID()

	// Build response
	response := map[string]interface{}{
		"jc":  c.joinRequestID,
		"vp":  volatilePaths,
		"cid": connectionID,
		"st":  time.Now().UnixMilli(),
	}

	data, _ := sonic.Marshal(response)
	c.sendDirect(data, true)
}

// handleAuthMessage is the unified auth handler for both Lark and Firebase.
// Waits for project config, validates token, sends response, signals completion.
func (c *ClientConn) handleAuthMessage(data []byte) {
	// Ensure we signal completion when done
	defer func() {
		if c.authDone != nil {
			close(c.authDone)
		}
		c.authProcessing = false
	}()

	// Wait for project config
	project, err := c.waitForProjectConfig()
	if err != nil {
		if c.protocol == ProtocolFirebase {
			// Try to parse request ID for error response
			var msg map[string]interface{}
			if sonic.Unmarshal(data, &msg) == nil {
				if d, ok := msg["d"].(map[string]interface{}); ok {
					if r, ok := d["r"].(float64); ok {
						c.sendFirebaseAuthError(int(r), "project not found")
					}
				}
			}
		} else {
			c.sendError("project not found")
		}
		return
	}

	// Store project config
	c.projectConfig = project

	// Parse request ID for response (needed for both success and error)
	var requestID int
	if c.protocol == ProtocolFirebase {
		var msg map[string]interface{}
		if sonic.Unmarshal(data, &msg) == nil {
			if d, ok := msg["d"].(map[string]interface{}); ok {
				if r, ok := d["r"].(float64); ok {
					requestID = int(r)
				}
			}
		}
	}

	// Validate auth
	authPayload, err := c.validateAuth(project)
	if err != nil {
		// Check if already sent (avoid double-send)
		if c.authResponseSent {
			return
		}
		c.authResponseSent = true

		// Send error response
		if c.protocol == ProtocolLark {
			c.sendLarkAuthError(auth.UserFriendlyError(err))
		} else if requestID > 0 {
			c.sendFirebaseAuthError(requestID, auth.UserFriendlyError(err))
		}
		return
	}

	// Check if already sent (avoid double-send)
	if c.authResponseSent {
		return
	}
	c.authResponseSent = true

	// Send success response based on protocol
	if c.protocol == ProtocolLark {
		c.sendLarkAuthAck(authPayload)
	} else if requestID > 0 {
		c.sendFirebaseOK(requestID, map[string]interface{}{
			"auth": map[string]interface{}{"uid": authPayload.UID},
		})
	}

	// If already connected to backend, send AUTH_CHANGED
	if c.backend != nil {
		authBytes, err := backend.EncodeAuthPayload(authPayload)
		if err != nil {
			logger.Error("Auth encode error", "client_id", c.id, "error", err)
			return
		}
		c.backend.SendMessage(&backend.Message{
			Type:     backend.MsgTypeAuthChanged,
			ClientID: c.id,
			Payload:  authBytes,
		})
	}
}

// sendLarkAuthAck sends the Lark AuthAck response
// Format: {"ac": "requestId", "au": "userId"}
func (c *ClientConn) sendLarkAuthAck(authPayload *backend.AuthPayload) {
	uid := ""
	if authPayload != nil {
		uid = authPayload.UID
	}
	response := map[string]interface{}{
		"ac": c.authRequestID,
		"au": uid,
	}
	data, _ := sonic.Marshal(response)
	c.sendDirect(data, true)
}

// sendLarkAuthError sends a Lark auth error response
// Format: {"ae": "requestId", "err": "error message"}
func (c *ClientConn) sendLarkAuthError(errMsg string) {
	response := map[string]interface{}{
		"ae":  c.authRequestID,
		"err": errMsg,
	}
	data, _ := sonic.Marshal(response)
	c.sendDirect(data, true)
}

// handleFirebaseAuth validates auth and sends Auth OK response
// Called asynchronously when Firebase client sends auth message
// formatFirebaseRequestID converts a Firebase request ID (float64) to string
func formatFirebaseRequestID(id float64) string {
	return strconv.Itoa(int(id))
}

// parseInt parses a string to int, returning error if not a valid integer
func parseInt(s string) (int, error) {
	return strconv.Atoi(s)
}

// isAuthMessage checks if a message is an auth message that should be handled locally
// Precomputed byte slices for fast auth detection (no allocations)
var (
	larkAuthPattern      = []byte(`"o":"au"`)
	firebaseAuthPattern  = []byte(`"a":"auth"`)
	firebaseGauthPattern = []byte(`"a":"gauth"`)
)

func (c *ClientConn) isAuthMessage(data []byte) bool {
	// Fast path: scan first 64 bytes for auth patterns before expensive JSON parse
	// Auth messages are small; a large message is definitely not auth
	header := data
	if len(header) > 64 {
		header = header[:64]
	}

	if c.protocol == ProtocolLark {
		if !bytes.Contains(header, larkAuthPattern) {
			return false
		}
	} else {
		if !bytes.Contains(header, firebaseAuthPattern) && !bytes.Contains(header, firebaseGauthPattern) {
			return false
		}
	}

	// Potential auth message - confirm with JSON parse
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		return false
	}

	if c.protocol == ProtocolLark {
		op, _ := msg["o"].(string)
		return op == "au"
	}

	// Firebase auth: {"t": "d", "d": {"a": "auth"|"gauth", ...}}
	t, _ := msg["t"].(string)
	if t != "d" {
		return false
	}
	d, _ := msg["d"].(map[string]interface{})
	if d == nil {
		return false
	}
	a, _ := d["a"].(string)
	return a == "auth" || a == "gauth"
}

// handleLateAuth handles an auth message that arrives after the client is already forwarding
// Validates the token and sends AUTH_CHANGED to backend
func (c *ClientConn) handleLateAuth(data []byte) {
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		// Invalid JSON - already forwarded to backend, nothing more to do
		return
	}

	// Extract token from message
	var token string
	var requestID int

	if c.protocol == ProtocolLark {
		// Lark auth: {"o": "au", "t": "<token>", "r": "<requestId>"}
		token, _ = msg["t"].(string)
		if rid, ok := msg["r"].(string); ok {
			// Request ID is a string in Lark protocol
			_ = rid // Will send response with same format
		}
	} else {
		// Firebase auth: {"t": "d", "d": {"r": 1, "a": "auth", "b": {"cred": "<token>"}}}
		d, _ := msg["d"].(map[string]interface{})
		if d != nil {
			if r, ok := d["r"].(float64); ok {
				requestID = int(r)
			}
			if b, ok := d["b"].(map[string]interface{}); ok {
				token, _ = b["cred"].(string)
			}
		}
	}

	// Fetch project config for validation (cached — secret keys rarely change)
	project, err := c.server.GetProjectCached(c.server.ctx, c.projectID)
	if err != nil {
		logger.Error("Late auth project lookup failed", "client_id", c.id, "project", c.projectID, "error", err)
		// Message already forwarded to backend, nothing more to do locally
		return
	}

	// Validate token
	var authPayload *backend.AuthPayload
	if token == "" {
		// Empty token = sign out to anonymous
		authPayload = &backend.AuthPayload{
			UID:      "",
			Provider: "anonymous",
			Claims:   make(map[string]any),
		}
		c.authToken = ""
		c.authInfo = nil
	} else {
		// Validate the token
		info, err := c.server.authValidator.ValidateForProject(
			token,
			project.SecretKey,
			project.AdminSecretKey,
			project.FirebaseProjectID,
		)
		if err != nil {
			logger.Debug("Late auth validation failed", "client_id", c.id, "error", auth.UserFriendlyError(err))
			// Send error response to client
			if c.protocol == ProtocolFirebase && requestID > 0 {
				c.sendFirebaseAuthError(requestID, auth.UserFriendlyError(err))
			}
			return
		}

		c.authToken = token
		c.authInfo = info
		authPayload = &backend.AuthPayload{
			UID:         info.UID,
			Provider:    info.Provider,
			Claims:      info.Token,
			IsTrueAdmin: info.IsTrueAdmin,
		}
	}

	// Send AUTH_CHANGED to backend
	authBytes, err := backend.EncodeAuthPayload(authPayload)
	if err != nil {
		logger.Error("Late auth encode error", "client_id", c.id, "error", err)
		return
	}

	err = c.backend.SendMessage(&backend.Message{
		Type:     backend.MsgTypeAuthChanged,
		ClientID: c.id,
		Payload:  authBytes,
	})
	if err != nil {
		logger.Error("Late auth failed to send AUTH_CHANGED", "client_id", c.id, "error", err)
	}

	// Send success response to client (late auth always sends response, no double-send check needed
	// since this is for auth that arrives after initial routing is complete)
	if c.protocol == ProtocolFirebase && requestID > 0 {
		c.sendFirebaseOK(requestID, map[string]interface{}{
			"auth": map[string]interface{}{"uid": authPayload.UID},
		})
	} else if c.protocol == ProtocolLark && c.authRequestID != "" {
		// Send Lark AuthAck from proxy
		c.sendLarkAuthAck(authPayload)
	}
	// Note: message is already forwarded to backend before this function is called
}

// sendFirebaseAuthError sends a Firebase auth error response
func (c *ClientConn) sendFirebaseAuthError(requestID int, message string) {
	response := map[string]interface{}{
		"t": "d",
		"d": map[string]interface{}{
			"r": requestID,
			"b": map[string]interface{}{
				"s": "error",
				"d": message,
			},
		},
	}
	responseData, _ := sonic.Marshal(response)
	c.sendDirect(responseData, true)
}

// maxForwardSize is the maximum message size the proxy will forward to a backend.
// This is a safety net — individual transports enforce tighter limits (e.g., 16MB
// for WebSocket/WebTransport), but this catches any gaps.
const maxForwardSize = 300 * 1024 * 1024 // 300MB

// forwardToBackend forwards a message to the backend
func (c *ClientConn) forwardToBackend(data []byte) {
	if c.backend == nil {
		return
	}

	if len(data) > maxForwardSize {
		logger.Warn("Message too large to forward", "client_id", c.id, "size", len(data))
		c.Close()
		return
	}

	err := c.backend.SendMessage(&backend.Message{
		Type:     backend.MsgTypeData,
		ClientID: c.id,
		Payload:  data,
	})
	if err != nil {
		logger.Error("Forward error", "client_id", c.id, "error", err)
	}
}

// sendError sends an error message to the client
func (c *ClientConn) sendError(message string) {
	var data []byte
	if c.protocol == ProtocolLark {
		data, _ = sonic.Marshal(map[string]interface{}{
			"type":  "error",
			"error": message,
		})
	} else {
		// Firebase error format
		data, _ = sonic.Marshal(map[string]interface{}{
			"t": "c",
			"d": map[string]interface{}{
				"t": "e",
				"d": message,
			},
		})
	}
	c.sendDirect(data, true)
}

// Close closes the client connection
func (c *ClientConn) Close() {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return
	}
	c.closed = true
	c.closeMu.Unlock()

	c.SetState(StateClosing)

	serverID := ""
	if c.backend != nil {
		serverID = c.backend.ServerID
	}
	logger.Debug("Client disconnected", "client_id", c.id, "project", c.projectID, "database", c.databaseID, "server", serverID)

	// Signal write goroutine to exit
	close(c.done)

	// Notify backend and unregister client from core mapping
	if c.backend != nil {
		c.backend.SendMessage(&backend.Message{
			Type:     backend.MsgTypeDisconnect,
			ClientID: c.id,
			Payload:  []byte{backend.DisconnectClean},
		})
		c.backend.UnregisterClient(c.id)
	}

	// Close transport
	c.transport.Close()

	// Unregister (nil check for tests)
	if c.server != nil {
		c.server.unregisterClient(c)
	}
}
