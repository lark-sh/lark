// SSE (Server-Sent Events) Transport Implementation
//
// This file implements the SSE transport for Firebase-compatible streaming clients.
// SSE provides real-time updates over HTTP for clients that can't use WebSocket.
//
// # How It Works
//
// SSE uses a long-lived HTTP response with Content-Type: text/event-stream:
//
//	GET /db/users.json
//	Accept: text/event-stream
//
//	HTTP/1.1 200 OK
//	Content-Type: text/event-stream
//
//	event: put
//	data: {"path":"/","data":{"name":"Alice"}}
//
//	event: put
//	data: {"path":"/name","data":"Bob"}
//
// The connection stays open, streaming events as they occur.
//
// # Subscription vs REST
//
// SSE differs from REST in how it handles data:
//   - REST: Single request-response, operation = "g" (get once)
//   - SSE: Creates subscription, operation = "sb" (subscribe)
//
// The subscription receives all changes to the path until the client disconnects.
//
// # Event Format
//
// Firebase SSE uses this event format:
//
//	event: <type>     // "put", "patch", or "keep-alive"
//	data: <json>      // {"path":"...","data":...}
//
// The proxy translates Lark events to this format:
//   - Lark: {"ev":"put","sp":"/users","p":"/123","v":{...}}
//   - SSE:  event: put\ndata: {"path":"/123","data":{...}}
//
// # Connection Management
//
// Unlike REST which pools connections, each SSE stream is a dedicated client:
//   - New SSETransport created per streaming request
//   - Subscribes to the requested path
//   - Streams events until client disconnects or timeout
//
// # Thread Safety
//
// Events are delivered via a buffered channel (64 events). If the client
// falls behind, the oldest events may be dropped. The transport uses
// sync.Once to ensure clean shutdown.
package proxy

import (
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
	ErrSSEClientClosed = errors.New("SSE client closed")
)

// SSETransport implements ClientTransport for SSE streaming clients.
// Unlike RESTTransport which correlates request/response by ID,
// SSETransport streams all events to the client.
type SSETransport struct {
	projectID  string
	databaseID string
	client     *ClientConn

	eventCh   chan []byte     // All events streamed here
	doneCh    chan struct{}   // Closed when connection should end
	closeOnce sync.Once

	mu     sync.Mutex
	closed bool
}

// NewSSETransport creates a new SSE transport for a specific database
func NewSSETransport(projectID, databaseID string) *SSETransport {
	return &SSETransport{
		projectID:  projectID,
		databaseID: databaseID,
		eventCh:    make(chan []byte, 64), // Buffer for events
		doneCh:     make(chan struct{}),
	}
}

// SetClient sets the client connection
func (t *SSETransport) SetClient(client *ClientConn) {
	t.client = client
}

// Send receives data from the backend and queues it for streaming
// Unlike REST, SSE streams all events (not filtered by request ID)
func (t *SSETransport) Send(data []byte, reliable bool) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	// Non-blocking send - drop oldest if buffer full
	select {
	case t.eventCh <- data:
	default:
		// Buffer full, drop oldest and add new
		select {
		case <-t.eventCh:
		default:
		}
		select {
		case t.eventCh <- data:
		default:
		}
	}

	return nil
}

// Close closes the transport
func (t *SSETransport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.mu.Unlock()

		close(t.doneCh)
		// Don't close eventCh here - let readers drain it first
	})
	return nil
}

// TransportType returns the transport protocol type
func (t *SSETransport) TransportType() byte {
	return backend.ProtocolREST // REST/SSE share 256MB response limit
}

// IsClosed returns whether the transport is closed
func (t *SSETransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// Events returns the channel for receiving events
func (t *SSETransport) Events() <-chan []byte {
	return t.eventCh
}

// Done returns a channel that's closed when the connection is closed
func (t *SSETransport) Done() <-chan struct{} {
	return t.doneCh
}

// =============================================================================
// SSE Virtual Client
// =============================================================================

// SSEVirtualClient wraps a ClientConn for SSE streaming
type SSEVirtualClient struct {
	transport *SSETransport
	client    *ClientConn
	ready     chan struct{} // Closed when client is ready (joined + database loaded)
	readyOnce sync.Once
	err       error // Set if setup failed

	mu        sync.Mutex
	requestID atomic.Uint32
}

// NewSSEVirtualClient creates a new virtual client for SSE streaming
func (s *Server) NewSSEVirtualClient(projectID, databaseID string, authInfo *auth.Info) (*SSEVirtualClient, error) {
	transport := NewSSETransport(projectID, databaseID)

	// Use server's allocateClientID to avoid collisions
	clientID := s.allocateClientID()
	if clientID == 0 {
		return nil, errors.New("too many concurrent connections")
	}

	client := newClientConn(s, clientID, transport, ProtocolLark)
	transport.SetClient(client)

	// Set project ID and database ID
	client.projectID = projectID
	client.databaseID = databaseID

	// Set auth info (will be included in CONNECT payload during routing)
	client.authInfo = authInfo

	// Fetch project config async
	client.fetchProjectConfig()

	// Register client with server so backend pool can route responses to it
	s.registerClient(client)

	// Start the write loop
	client.Start()

	vc := &SSEVirtualClient{
		transport: transport,
		client:    client,
		ready:     make(chan struct{}),
	}

	// Start the setup process in background
	go vc.setup()

	return vc, nil
}

// setup performs the async setup: send join, wait for database loaded
func (vc *SSEVirtualClient) setup() {
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

	// Process the join message through the client
	vc.client.OnMessage(joinData, true)

	// Trigger routing
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
func (vc *SSEVirtualClient) WaitReady(timeout time.Duration) error {
	select {
	case <-vc.ready:
		return vc.err
	case <-time.After(timeout):
		return errors.New("timeout waiting for client to be ready")
	}
}

// nextRequestID generates a unique request ID
func (vc *SSEVirtualClient) nextRequestID() string {
	return fmt.Sprintf("sse-%d", vc.requestID.Add(1))
}

// Subscribe sends a subscribe message for the given path
func (vc *SSEVirtualClient) Subscribe(path string) error {
	reqID := vc.nextRequestID()

	// Build subscribe message
	// Lark format: {"o":"sb","p":"/path","e":["value"],"r":"request-id"}
	msg := map[string]interface{}{
		"o": "sb",
		"p": path,
		"e": []string{"value"}, // Subscribe to value events
		"r": reqID,
	}

	msgData, err := sonic.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe: %w", err)
	}

	// Send through the client
	vc.client.OnMessage(msgData, true)

	return nil
}

// Events returns the channel for receiving events
func (vc *SSEVirtualClient) Events() <-chan []byte {
	return vc.transport.Events()
}

// Done returns a channel that's closed when the connection is closed
func (vc *SSEVirtualClient) Done() <-chan struct{} {
	return vc.transport.Done()
}

// Close closes the virtual client
func (vc *SSEVirtualClient) Close() {
	vc.client.Close()
	vc.transport.Close()
}

// Transport returns the underlying transport
func (vc *SSEVirtualClient) Transport() *SSETransport {
	return vc.transport
}

// =============================================================================
// SSE Event Parsing
// =============================================================================

// SSEEvent represents a parsed SSE event from the Lark protocol
type SSEEvent struct {
	Type string      // "put", "patch", or "keep-alive"
	Path string      // Relative path
	Data interface{} // The data value
}

// ParseSSEEvent parses a Lark protocol message into an SSE event
// Returns nil if the message is not a relevant event (e.g., ack, nack, join confirm)
func ParseSSEEvent(data []byte) *SSEEvent {
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		return nil
	}

	// Check for event message (ev field)
	evType, ok := msg["ev"].(string)
	if !ok {
		// Not an event message - could be ack, nack, join confirm, etc.
		// Log non-event messages for debugging
		if _, hasA := msg["a"]; hasA {
			// Ack - ignore (subscribe confirmation)
			return nil
		}
		if _, hasN := msg["n"]; hasN {
			// Nack - could return as error event
			logger.Debug("SSE received NACK", "data", string(data))
			return nil
		}
		if _, hasJC := msg["jc"]; hasJC {
			// Join confirm - ignore
			return nil
		}
		logger.Debug("SSE unknown message type", "data", string(data))
		return nil
	}

	// Get path (relative to subscription)
	path, _ := msg["p"].(string)
	if path == "" {
		path = "/"
	}

	// Get value
	value := msg["v"]

	// Map event type
	switch evType {
	case "put":
		return &SSEEvent{Type: "put", Path: path, Data: value}
	case "patch":
		return &SSEEvent{Type: "patch", Path: path, Data: value}
	case "vb":
		// Volatile batch - expand into individual events
		// For SSE, we'll send each batch item as a separate put
		// This is simplified - the full implementation would iterate through b[sp][p] = v
		return nil // TODO: Handle volatile batches if needed
	default:
		logger.Debug("SSE unknown event type", "event_type", evType)
		return nil
	}
}
