// WebSocket Transport Implementation
//
// This file implements the WebSocket transport for client connections. WebSocket is
// the primary transport for browser clients using the Lark SDK or Firebase SDK.
//
// # Connection Types
//
// Two WebSocket handlers exist:
//   - handleWebSocket: Modern Lark protocol clients
//   - handleFirebaseWebSocket: Firebase-compatible clients (with handshake)
//
// The Firebase handler sends a "hello" message with session info before the
// client starts sending data. This mimics Firebase RTDB behavior.
//
// # Keep-Alive
//
// WebSocket connections use ping/pong for keep-alive:
//   - Server sends ping every 30 seconds
//   - Client has 60 seconds to respond with pong
//   - Read deadline is reset on any message or pong
//
// # Long Polling → WebSocket Upgrade
//
// Firebase clients may start with Long Polling (if WebSocket previously failed)
// and later attempt to upgrade to WebSocket. Two strategies handle this:
//
// handleFirebaseWebSocketResume (disabled):
//   - Requires sticky sessions (LP and WS on same proxy)
//   - Swaps transport without interrupting client state
//   - Currently disabled due to multi-proxy architecture
//
// handleFirebaseWebSocketFakeUpgrade (active):
//   - Works without sticky sessions
//   - Sends PONGs to clear client's "WS failed" flag
//   - After SWITCH_ACK, sends RESET to force clean reconnect
//   - Client reconnects using WebSocket directly
//
// # Thread Safety
//
// WebSocket writes are protected by writeMu mutex. Multiple goroutines can
// safely call Send() concurrently. The read loop runs in a single goroutine.
package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"

	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/logger"
)

// WebSocketTransport handles a WebSocket client connection
type WebSocketTransport struct {
	conn   *websocket.Conn
	client *ClientConn

	writeMu sync.Mutex
	closed  bool
}

// NewWebSocketTransport creates a new WebSocket transport
func NewWebSocketTransport(conn *websocket.Conn) *WebSocketTransport {
	return &WebSocketTransport{
		conn: conn,
	}
}

// SetClient sets the client connection
func (t *WebSocketTransport) SetClient(client *ClientConn) {
	t.client = client
}

// Send sends data to the client
func (t *WebSocketTransport) Send(data []byte, reliable bool) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if t.closed {
		return nil
	}

	t.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return t.conn.WriteMessage(websocket.TextMessage, data)
}

// Close closes the connection
func (t *WebSocketTransport) Close() error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	return t.conn.Close()
}

// TransportType returns the transport protocol type
func (t *WebSocketTransport) TransportType() byte {
	return backend.ProtocolWebSocket
}

// ReadLoop reads messages from the WebSocket
func (t *WebSocketTransport) ReadLoop() {
	defer t.client.Close()

	t.conn.SetReadLimit(16 * 1024 * 1024) // 16MB max message
	t.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	t.conn.SetPongHandler(func(string) error {
		t.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Start ping loop to keep connection alive
	go t.pingLoop()

	for {
		_, message, err := t.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Debug("WS read error", "client_id", t.client.id, "error", err)
			}
			return
		}

		t.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		t.client.OnMessage(message, true) // WebSocket is always reliable
	}
}

// pingLoop sends periodic WebSocket pings to keep the connection alive
func (t *WebSocketTransport) pingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.writeMu.Lock()
			if t.closed {
				t.writeMu.Unlock()
				return
			}
			t.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := t.conn.WriteMessage(websocket.PingMessage, nil)
			t.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// handleWebSocket handles incoming WebSocket connections (modern Lark protocol)
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Warn("WS upgrade error", "error", err)
		return
	}

	transport := NewWebSocketTransport(conn)
	clientID := s.allocateClientID()
	if clientID == 0 {
		logger.Warn("WS connection rejected: too many concurrent connections")
		conn.Close()
		return
	}
	client := newClientConn(s, clientID, transport, ProtocolLark)
	transport.SetClient(client)

	// Extract project ID (and optional database ID) from subdomain
	projectID, subdomainDB := s.extractProjectAndDatabase(r.Host)
	client.projectID = projectID
	if client.projectID == "" {
		logger.Warn("WS no project ID in request", "host", r.Host)
		conn.Close()
		return
	}
	if subdomainDB != "" {
		client.databaseID = subdomainDB
	}

	// Start fetching project config immediately (async)
	client.fetchProjectConfig()

	s.registerClient(client)

	// Start client's write goroutine
	client.Start()

	// Start read loop (blocks until connection closes)
	transport.ReadLoop()
}

// handleFirebaseWebSocket handles Firebase-compatible WebSocket connections
func (s *Server) handleFirebaseWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Warn("Firebase WS upgrade error", "error", err)
		return
	}

	// Check for session resumption (LP→WS upgrade)
	// When a client upgrades from Long Polling to WebSocket, it passes s=<sessionId>
	//
	// We always use the "fake upgrade then reset" strategy here, even if we have the LP session
	// on this proxy. This ensures consistent behavior across multiple proxies without sticky sessions.
	// The fake upgrade clears the client's previous_websocket_failure flag and forces a clean WS reconnect.
	//
	// TODO: Re-enable real upgrade (handleFirebaseWebSocketResume) when we have sticky sessions
	resumeSessionID := r.URL.Query().Get("s")
	if resumeSessionID != "" {
		logger.Debug("Firebase LP→WS upgrade requested, using fake upgrade", "session_id", resumeSessionID)
		s.handleFirebaseWebSocketFakeUpgrade(conn, r.Host)
		return
	}

	transport := NewWebSocketTransport(conn)
	clientID := s.allocateClientID()
	if clientID == 0 {
		logger.Warn("Firebase WS connection rejected: too many concurrent connections")
		conn.Close()
		return
	}
	client := newClientConn(s, clientID, transport, ProtocolFirebase)
	transport.SetClient(client)

	// Store hostname for metadata
	client.hostname = r.Host

	// Extract project ID (and optional database ID) from hostname or ns= query param
	projectID, subdomainDB := s.extractProjectAndDatabase(r.Host)
	client.projectID = projectID
	if client.projectID == "" {
		// Fall back to ns= query param
		client.projectID = r.URL.Query().Get("ns")
	}
	if subdomainDB != "" {
		client.databaseID = subdomainDB
	}

	if client.projectID == "" {
		logger.Warn("Firebase WS no project ID in request")
		conn.Close()
		return
	}

	// Start fetching project config immediately (async)
	client.fetchProjectConfig()

	s.registerClient(client)

	// Start client's write goroutine
	client.Start()

	// Send Firebase hello (using sendDirect since writeLoop hasn't started reading yet)
	sessionID := generateSessionID()
	client.firebaseState.SessionID = sessionID

	hello := map[string]interface{}{
		"t": "c",
		"d": map[string]interface{}{
			"t": "h",
			"d": map[string]interface{}{
				"ts": time.Now().UnixMilli(),
				"v":  "5",
				"h":  r.Host,
				"s":  sessionID,
			},
		},
	}
	helloData, _ := sonic.Marshal(hello)
	client.sendDirect(helloData, true)

	// Start read loop (blocks until connection closes)
	transport.ReadLoop()
}

// handleFirebaseWebSocketResume handles a WebSocket connection that is resuming an LP session
// This is called when the client passes s=<sessionId> to upgrade from Long Polling to WebSocket
//
// IMPORTANT: The Firebase SDK upgrade flow is:
// 1. WS connects with s=<sessionId> - this is a "secondary" connection being TESTED
// 2. Server sends PONGs on WS for health checks
// 3. Client is STILL reading from LP and sending on LP
// 4. When WS is "healthy" (enough PONGs), client sends SWITCH_ACK on WS
// 5. Client sends END_TRANSMISSION on LP
// 6. Server sends END_TRANSMISSION back on LP
// 7. Only THEN does client switch to WS
//
// We must NOT swap the transport until we receive SWITCH_ACK!
func (s *Server) handleFirebaseWebSocketResume(conn *websocket.Conn, lpSession *LongPollSession) {
	logger.Debug("Firebase secondary WS connection opened for LP session", "lp_session_id", lpSession.ID, "firebase_session_id", lpSession.client.firebaseState.SessionID)

	// Get the client (still attached to LP)
	client := lpSession.client

	// Configure WebSocket for the health check phase
	conn.SetReadLimit(16 * 1024 * 1024) // 16MB max message
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Send PONGs immediately for health check
	// The client needs these to consider the WS connection "healthy"
	pong := []byte(`{"t":"c","d":{"t":"o","d":{}}}`)
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteMessage(websocket.TextMessage, pong)
	conn.WriteMessage(websocket.TextMessage, pong)

	logger.Debug("Firebase secondary WS: sent PONGs, waiting for SWITCH_ACK")

	// Start reading from WS, waiting for SWITCH_ACK
	// During this phase, LP is still the primary transport
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Debug("Firebase secondary WS read error before SWITCH_ACK", "error", err)
			conn.Close()
			return
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// Parse the message to check for SWITCH_ACK or PING
		var msg map[string]interface{}
		if err := sonic.Unmarshal(message, &msg); err != nil {
			logger.Debug("Firebase secondary WS received invalid JSON", "message", string(message))
			continue
		}

		t, _ := msg["t"].(string)
		d, _ := msg["d"].(map[string]interface{})

		if t == "c" && d != nil {
			ct, _ := d["t"].(string)
			switch ct {
			case "p": // PING - respond with PONG
				logger.Debug("Firebase secondary WS: received PING, sending PONG")
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				conn.WriteMessage(websocket.TextMessage, pong)
				continue

			case "a": // SWITCH_ACK - client is ready to switch!
				logger.Debug("Firebase secondary WS: received SWITCH_ACK, performing transport swap")

				// NOW we swap the transport
				transport := NewWebSocketTransport(conn)
				client.SwapTransport(transport)
				transport.SetClient(client)

				// Mark LP session as upgraded
				lpSession.mu.Lock()
				lpSession.upgraded = true
				lpSession.client = nil // Detach client so LP Close() doesn't close the client
				lpSession.mu.Unlock()

				logger.Debug("Firebase client upgraded from LP to WS", "client_id", client.id, "lp_session_id", lpSession.ID)

				// Start normal WS read loop (blocks until connection closes)
				transport.ReadLoop()
				return
			}
		}

		// Any other message before SWITCH_ACK is unexpected
		logger.Debug("Firebase secondary WS: unexpected message before SWITCH_ACK", "message", string(message))
	}
}

// handleFirebaseWebSocketFakeUpgrade handles a WS connection that's trying to upgrade from LP,
// but we don't have the LP session (likely on a different proxy).
//
// Strategy: "Fake Upgrade Then Reset"
// 1. Send PONGs so the client thinks WS is healthy (clears previous_websocket_failure flag)
// 2. When SWITCH_ACK arrives, send CONTROL_RESET to force a fresh reconnect
// 3. Client reconnects, now uses WS directly since the failure flag is cleared
//
// This allows clients stuck on LP (due to previous WS failure) to get back onto WS,
// while still supporting "true" LP for clients that actually need it.
func (s *Server) handleFirebaseWebSocketFakeUpgrade(conn *websocket.Conn, host string) {
	logger.Debug("Firebase fake upgrade starting")

	// Configure WebSocket
	conn.SetReadLimit(16 * 1024 * 1024)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Send PONGs for health check - this clears the previous_websocket_failure flag on the client
	// The client needs 2 PONGs to consider the connection "healthy"
	pong := []byte(`{"t":"c","d":{"t":"o","d":{}}}`)
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, pong); err != nil {
		logger.Debug("Firebase fake upgrade: failed to send PONG", "error", err)
		conn.Close()
		return
	}
	if err := conn.WriteMessage(websocket.TextMessage, pong); err != nil {
		logger.Debug("Firebase fake upgrade: failed to send PONG", "error", err)
		conn.Close()
		return
	}

	logger.Debug("Firebase fake upgrade: sent PONGs, waiting for SWITCH_ACK")

	// Wait for SWITCH_ACK
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Debug("Firebase fake upgrade: read error", "error", err)
			conn.Close()
			return
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// Parse message
		var msg map[string]interface{}
		if err := sonic.Unmarshal(message, &msg); err != nil {
			logger.Debug("Firebase fake upgrade: invalid JSON", "message", string(message))
			continue
		}

		t, _ := msg["t"].(string)
		d, _ := msg["d"].(map[string]interface{})

		if t == "c" && d != nil {
			ct, _ := d["t"].(string)
			switch ct {
			case "p": // PING - respond with PONG
				logger.Debug("Firebase fake upgrade: received PING, sending PONG")
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				conn.WriteMessage(websocket.TextMessage, pong)
				continue

			case "a": // SWITCH_ACK - client is ready to switch, send RESET
				logger.Debug("Firebase fake upgrade: received SWITCH_ACK, sending RESET")

				// Send CONTROL_RESET - tells client to reconnect fresh
				// The client's previous_websocket_failure flag is now cleared,
				// so it will reconnect using WS directly
				reset := []byte(`{"t":"c","d":{"t":"r","d":""}}`)
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, reset); err != nil {
					logger.Debug("Firebase fake upgrade: failed to send RESET", "error", err)
				}

				// Close the connection - client will reconnect fresh on WS
				conn.Close()
				logger.Debug("Firebase fake upgrade complete, client should reconnect on WS")
				return
			}
		}

		// Ignore other messages during fake upgrade
		logger.Debug("Firebase fake upgrade: ignoring message", "message", string(message))
	}
}

// generateSessionID creates a random session ID
func generateSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
