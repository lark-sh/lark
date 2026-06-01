// REST Transport Implementation
//
// This file implements the REST transport for Firebase-compatible REST API clients.
// REST is treated as just another transport type - the proxy translates HTTP
// requests into Lark protocol messages and vice versa.
//
// # Request Flow
//
// 1. HTTP request arrives (GET /db/users/123.json)
// 2. RESTClientPool provides a RESTVirtualClient for this project/database
// 3. Virtual client translates HTTP method to Lark operation:
//   - GET    → "o":"g" (get)
//   - PUT    → "o":"s" (set)
//   - POST   → "o":"p" (push, generates key)
//   - PATCH  → "o":"u" (update/merge)
//   - DELETE → "o":"r" (remove)
//
// 4. Request sent to backend with unique request ID
// 5. RESTTransport waits for response with matching request ID
// 6. Response translated to HTTP (status code, JSON body)
//
// # Connection Reuse
//
// Unlike WebSocket where each connection is a separate client, REST requests
// can share a backend connection:
//
//   - RESTClientPool maintains virtual clients per project/database
//   - Multiple REST requests reuse the same virtual client
//   - Idle clients are cleaned up after 10 seconds
//
// # Response Correlation
//
// Each REST request gets a unique request ID. The transport maintains a map
// of pending request IDs → response channels. When a response arrives, it's
// routed to the waiting goroutine by matching the "r" (request ID) field.
//
// # Error Mapping
//
// Lark protocol errors are mapped to HTTP status codes:
//   - permission_denied → 403 Forbidden
//   - not_found → 404 Not Found
//   - invalid_data → 400 Bad Request
//   - unavailable → 503 Service Unavailable
//   - timeout → 504 Gateway Timeout
//
// See larkdb.go for the HTTP handler implementation.
package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"

	"github.com/lark-sh/lark/edge/auth"
	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/logger"
)

var (
	ErrRESTTimeout      = errors.New("REST request timed out")
	ErrRESTClientClosed = errors.New("REST client closed")
	ErrResponseMismatch = errors.New("unexpected response format")
)

// RESTTransport implements ClientTransport for REST clients.
// Unlike WebSocket/WebTransport, REST is request-response, so this transport
// buffers responses and allows callers to wait for specific request IDs.
type RESTTransport struct {
	projectID  string
	databaseID string
	client     *ClientConn

	mu         sync.Mutex
	closed     bool
	lastActive time.Time

	// Response handling
	responsesMu sync.Mutex
	responses   map[string]chan []byte // requestID -> response channel
}

// NewRESTTransport creates a new REST transport for a specific database
func NewRESTTransport(projectID, databaseID string) *RESTTransport {
	return &RESTTransport{
		projectID:  projectID,
		databaseID: databaseID,
		lastActive: time.Now(),
		responses:  make(map[string]chan []byte),
	}
}

// SetClient sets the client connection
func (t *RESTTransport) SetClient(client *ClientConn) {
	t.client = client
}

// Send receives data from the backend (responses to our requests)
// This is called by ClientConn when data comes back from the server
func (t *RESTTransport) Send(data []byte, reliable bool) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	// Parse the response to get the request ID
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		logger.Debug("REST failed to parse response", "error", err)
		return nil
	}

	// Extract request ID - could be in different fields depending on message type:
	// See LARK_WIRE_PROTOCOL.md for response formats
	// - "r" for regular responses
	// - "jc" for JoinConfirm (JoinAck)
	// - "ac" for AuthConfirm (AuthAck)
	// - "ae" for AuthError
	// - "a" for Ack (value is the request ID)
	// - "n" for Nack (value is the request ID)
	// - "oc" for Once response (value is the request ID)
	var requestID string
	if r, ok := msg["r"].(string); ok {
		requestID = r
	} else if r, ok := msg["r"].(float64); ok {
		requestID = fmt.Sprintf("%.0f", r)
	} else if jc, ok := msg["jc"].(string); ok {
		requestID = jc
	} else if ac, ok := msg["ac"].(string); ok {
		requestID = ac
	} else if ae, ok := msg["ae"].(string); ok {
		requestID = ae
	} else if a, ok := msg["a"].(string); ok {
		requestID = a
	} else if n, ok := msg["n"].(string); ok {
		requestID = n
	} else if oc, ok := msg["oc"].(string); ok {
		requestID = oc
	}

	if requestID == "" {
		// No request ID - might be a server push or error, log and ignore
		logger.Debug("REST response without request ID", "data", string(data))
		return nil
	}

	// Route to the waiting channel
	t.responsesMu.Lock()
	ch, ok := t.responses[requestID]
	t.responsesMu.Unlock()

	if ok {
		select {
		case ch <- data:
		default:
			// Channel full, drop (shouldn't happen with buffered channel)
			logger.Warn("REST response channel full", "request_id", requestID)
		}
	} else {
		logger.Debug("REST no waiter for request", "request_id", requestID)
	}

	return nil
}

// Close closes the transport
func (t *RESTTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	// Close all waiting response channels
	t.responsesMu.Lock()
	for _, ch := range t.responses {
		close(ch)
	}
	t.responses = make(map[string]chan []byte)
	t.responsesMu.Unlock()

	return nil
}

// TransportType returns the transport protocol type
func (t *RESTTransport) TransportType() byte {
	return backend.ProtocolREST
}

// IsClosed returns whether the transport is closed
func (t *RESTTransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// Touch updates the last active time
func (t *RESTTransport) Touch() {
	t.mu.Lock()
	t.lastActive = time.Now()
	t.mu.Unlock()
}

// LastActive returns the last active time
func (t *RESTTransport) LastActive() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastActive
}

// RegisterRequest creates a channel to wait for a response with the given request ID
func (t *RESTTransport) RegisterRequest(requestID string) chan []byte {
	ch := make(chan []byte, 1)
	t.responsesMu.Lock()
	t.responses[requestID] = ch
	t.responsesMu.Unlock()
	return ch
}

// UnregisterRequest removes a request from the waiting map
func (t *RESTTransport) UnregisterRequest(requestID string) {
	t.responsesMu.Lock()
	delete(t.responses, requestID)
	t.responsesMu.Unlock()
}

// WaitForResponse waits for a response with the given request ID
func (t *RESTTransport) WaitForResponse(requestID string, timeout time.Duration) ([]byte, error) {
	ch := t.RegisterRequest(requestID)
	defer t.UnregisterRequest(requestID)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case data, ok := <-ch:
		if !ok {
			return nil, ErrRESTClientClosed
		}
		return data, nil
	case <-timer.C:
		return nil, ErrRESTTimeout
	}
}

// =============================================================================
// REST Client Pool
// =============================================================================

// RESTClientPool manages virtual clients for REST requests.
// Multiple REST requests to the same database share a single virtual client.
type RESTClientPool struct {
	server *Server

	mu      sync.RWMutex
	clients map[string]*RESTVirtualClient // key: "projectID/databaseID"

	idleTimeout time.Duration
	// Note: We use server.allocateClientID() for client IDs to avoid collisions
	// with WebSocket/WebTransport clients
}

// RESTVirtualClient wraps a ClientConn for REST usage
type RESTVirtualClient struct {
	transport *RESTTransport
	client    *ClientConn
	ready     chan struct{} // Closed when client is ready (joined + database loaded)
	readyOnce sync.Once
	err       error // Set if setup failed

	mu        sync.Mutex
	requestID atomic.Uint32
}

// NewRESTClientPool creates a new REST client pool
func NewRESTClientPool(server *Server, idleTimeout time.Duration) *RESTClientPool {
	pool := &RESTClientPool{
		server:      server,
		clients:     make(map[string]*RESTVirtualClient),
		idleTimeout: idleTimeout,
	}

	// Start cleanup goroutine
	go pool.cleanupLoop()

	return pool
}

// GetOrCreate gets an existing virtual client or creates a new one
// authToken is the raw JWT (empty for anonymous), authInfo is the validated auth (nil for anonymous)
func (p *RESTClientPool) GetOrCreate(projectID, databaseID, authToken string, authInfo *auth.Info) (*RESTVirtualClient, error) {
	// Pool key includes hash of auth token so different tokens get different clients
	// This handles cases where same UID has different claims
	authHash := ""
	if authToken != "" {
		hash := sha256.Sum256([]byte(authToken))
		authHash = hex.EncodeToString(hash[:8]) // First 8 bytes = 16 hex chars, enough for uniqueness
	}
	key := projectID + "/" + databaseID + "/" + authHash

	// Fast path: check if client exists
	p.mu.RLock()
	vc, ok := p.clients[key]
	p.mu.RUnlock()

	if ok && !vc.transport.IsClosed() {
		vc.transport.Touch()
		return vc, nil
	}

	// Slow path: create new client
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if vc, ok := p.clients[key]; ok && !vc.transport.IsClosed() {
		vc.transport.Touch()
		return vc, nil
	}

	// Create new virtual client
	vc, err := p.createVirtualClient(projectID, databaseID, authInfo)
	if err != nil {
		return nil, err
	}

	p.clients[key] = vc
	return vc, nil
}

// createVirtualClient creates a new virtual client for REST
// authInfo is the validated auth (nil for anonymous)
func (p *RESTClientPool) createVirtualClient(projectID, databaseID string, authInfo *auth.Info) (*RESTVirtualClient, error) {
	transport := NewRESTTransport(projectID, databaseID)

	// Use server's allocateClientID to avoid collisions with WebSocket/WebTransport clients
	clientID := p.server.allocateClientID()
	if clientID == 0 {
		return nil, errors.New("too many concurrent connections")
	}

	client := newClientConn(p.server, clientID, transport, ProtocolLark)
	transport.SetClient(client)

	// Set project ID (normally comes from subdomain)
	client.projectID = projectID
	client.databaseID = databaseID

	// Set auth info (will be included in CONNECT payload during routing)
	// nil authInfo means anonymous
	client.authInfo = authInfo

	// Fetch project config async
	client.fetchProjectConfig()

	// Register client with server so backend pool can route responses to it
	// This is critical - without this, GetClient() returns nil and responses are dropped
	p.server.registerClient(client)

	// Start the write loop (delivers backend responses to the transport)
	client.Start()

	vc := &RESTVirtualClient{
		transport: transport,
		client:    client,
		ready:     make(chan struct{}),
	}

	// Start the setup process in background
	go vc.setup()

	return vc, nil
}

// setup performs the async setup: send join, wait for join ack, wait for database loaded
func (vc *RESTVirtualClient) setup() {
	defer vc.readyOnce.Do(func() { close(vc.ready) })

	// Wait for project config to be fetched
	select {
	case <-vc.client.projectReady:
	case <-time.After(10 * time.Second):
		vc.err = errors.New("timeout waiting for project config")
		return
	}

	// Check if project was found
	if vc.client.projectConfig == nil {
		vc.err = errors.New("project not found")
		return
	}

	// Send JOIN message
	joinReqID := vc.nextRequestID()
	joinMsg := map[string]interface{}{
		"o": "j",
		"d": vc.client.projectID + "/" + vc.client.databaseID,
		"r": joinReqID,
	}
	joinData, _ := sonic.Marshal(joinMsg)

	// Register to wait for join ack
	joinCh := vc.transport.RegisterRequest(joinReqID)
	defer vc.transport.UnregisterRequest(joinReqID)

	// Process the join message through the client
	vc.client.OnMessage(joinData, true)

	// Wait for join ack (comes from proxy, not backend)
	select {
	case _, ok := <-joinCh:
		if !ok {
			vc.err = errors.New("client closed during join")
			return
		}
		// Join ack received
	case <-time.After(10 * time.Second):
		vc.err = errors.New("timeout waiting for join ack")
		return
	}

	// Now trigger routing - normally this happens when a "real" operation arrives,
	// but for REST we need to trigger it explicitly so the backend connection is ready
	// before the first operation is sent.
	if vc.client.State() == StateConnected && vc.client.databaseID != "" {
		vc.client.startRouting()
	}

	// Wait for client to reach Forwarding state (database loaded)
	for i := 0; i < 100; i++ { // 10 second timeout
		if vc.client.State() == StateForwarding {
			return // Ready!
		}
		if vc.client.State() == StateClosing {
			vc.err = errors.New("client closed during setup")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	vc.err = errors.New("timeout waiting for database to load")
}

// WaitReady waits for the virtual client to be ready
func (vc *RESTVirtualClient) WaitReady(timeout time.Duration) error {
	select {
	case <-vc.ready:
		return vc.err
	case <-time.After(timeout):
		return errors.New("timeout waiting for client to be ready")
	}
}

// nextRequestID generates a unique request ID
func (vc *RESTVirtualClient) nextRequestID() string {
	return fmt.Sprintf("rest-%d", vc.requestID.Add(1))
}

// SendOperation sends a Lark operation and waits for the response
// opts contains query parameters to forward to the backend (orderBy, limitTo*, startAt, endAt, equalTo, shallow)
func (vc *RESTVirtualClient) SendOperation(op string, path string, value interface{}, opts *RESTQueryOptions, timeout time.Duration) ([]byte, error) {
	vc.transport.Touch()

	reqID := vc.nextRequestID()

	// Build the operation message
	// Modern Lark format: {"o":"<op>","p":"<path>","v":<value>,"r":"<requestId>"}
	// See LARK_WIRE_PROTOCOL.md - path is in "p", not "d"
	// "d" is only used for JOIN messages (database ID)
	msg := map[string]interface{}{
		"o": op,
		"p": path,
		"r": reqID,
	}

	// Add value for write operations
	if value != nil {
		msg["v"] = value
	}

	// Add query options for read operations
	if opts != nil {
		// Shallow read
		if opts.Shallow {
			msg["sh"] = true
		}

		// Ordering - use orderBy for special values, orderByChild for child paths
		if opts.OrderBy != "" {
			msg["orderBy"] = opts.OrderBy
		}
		if opts.OrderByChild != "" {
			msg["orderByChild"] = opts.OrderByChild
		}

		// Limits
		if opts.LimitToFirst != nil {
			msg["limitToFirst"] = *opts.LimitToFirst
		}
		if opts.LimitToLast != nil {
			msg["limitToLast"] = *opts.LimitToLast
		}

		// Range filters
		if opts.StartAt != nil {
			msg["startAt"] = opts.StartAt
			if opts.StartAtKey != "" {
				msg["startAtKey"] = opts.StartAtKey
			}
		}
		if opts.EndAt != nil {
			msg["endAt"] = opts.EndAt
			if opts.EndAtKey != "" {
				msg["endAtKey"] = opts.EndAtKey
			}
		}
		if opts.EqualTo != nil {
			msg["equalTo"] = opts.EqualTo
			if opts.EqualToKey != "" {
				msg["equalToKey"] = opts.EqualToKey
			}
		}

		// Conditional write (CAS) - include hash for compare-and-swap
		if opts.ETag != "" {
			msg["h"] = opts.ETag
			msg["hp"] = true
		}
	}

	msgData, err := sonic.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal operation: %w", err)
	}

	// Register for response before sending
	respCh := vc.transport.RegisterRequest(reqID)
	defer vc.transport.UnregisterRequest(reqID)

	// Send through the client (will forward to backend)
	vc.client.OnMessage(msgData, true)

	// Wait for response
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case resp, ok := <-respCh:
		if !ok {
			return nil, ErrRESTClientClosed
		}
		return resp, nil
	case <-timer.C:
		return nil, ErrRESTTimeout
	}
}

// Close closes the virtual client
func (vc *RESTVirtualClient) Close() {
	vc.client.Close()
	vc.transport.Close()
}

// cleanupLoop periodically removes idle clients
func (p *RESTClientPool) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()
		now := time.Now()
		for key, vc := range p.clients {
			if now.Sub(vc.transport.LastActive()) > p.idleTimeout {
				logger.Debug("REST pool removing idle client", "key", key)
				vc.Close()
				delete(p.clients, key)
			}
		}
		p.mu.Unlock()
	}
}

// Close closes all clients in the pool
func (p *RESTClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, vc := range p.clients {
		vc.Close()
	}
	p.clients = make(map[string]*RESTVirtualClient)
}
