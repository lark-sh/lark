// Long Polling Transport Implementation
//
// This file implements Firebase-compatible Long Polling for clients that cannot
// use WebSocket (corporate firewalls, legacy browsers, etc.).
//
// # How Long Polling Works
//
// Unlike WebSocket's persistent connection, Long Polling uses HTTP request-response:
//
//  1. Client makes GET request and waits (up to 30 seconds)
//  2. Server holds request open until data is available or timeout
//  3. Server responds with any queued messages
//  4. Client immediately makes another GET request
//  5. Client sends data via separate POST requests
//
// This creates a "simulated" bidirectional channel over HTTP/1.1.
//
// # Session Management
//
// Each Long Poll client has a LongPollSession that tracks:
//   - Pending messages to send to client
//   - Frame counter for ordering (Firebase protocol requirement)
//   - Active GET request (for immediate delivery)
//   - Password for request authentication
//
// Sessions are stored in LongPollPool and have a 60-second idle timeout.
//
// # Message Numbering
//
// Firebase LP protocol uses frame numbers for ordering and deduplication:
//   - Server assigns sequential frame numbers to each message batch
//   - Client can request frames starting from a specific number
//   - This handles message loss during network issues
//
// # LP → WebSocket Upgrade
//
// Firebase SDK tracks WebSocket failures in localStorage. If WS fails, it falls
// back to LP. When LP is working, the SDK periodically attempts WS upgrade:
//
//  1. Client opens WS with ?s=<sessionId>
//  2. Server sends PONGs for health check
//  3. Client sends SWITCH_ACK when WS is "healthy"
//  4. Server can then migrate the session (see websocket.go)
//
// # Thread Safety
//
// LongPollSession uses mutex for all state access. The transport wraps the
// session and delegates thread safety to it.
//
// # Goal
// 
// The goal of this package is just to implement enough LongPolling support to get a legacy Firebase client to switch from LP->WS
// We aren't really trying to support Long Polling as a first-class transport option.
package proxy

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"

	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/logger"
)

// =============================================================================
// Long Polling Transport
// =============================================================================

// LongPollTransport implements ClientTransport for Long Polling clients.
// Unlike WebSocket, LP is request-response based, so we queue messages
// and deliver them when the client polls.
type LongPollTransport struct {
	session *LongPollSession
	client  *ClientConn

	mu     sync.Mutex
	closed bool
}

// NewLongPollTransport creates a new Long Poll transport for a session
func NewLongPollTransport(session *LongPollSession) *LongPollTransport {
	return &LongPollTransport{
		session: session,
	}
}

// SetClient sets the client connection
func (t *LongPollTransport) SetClient(client *ClientConn) {
	t.client = client
}

// Send queues data to be delivered to the client on their next poll
func (t *LongPollTransport) Send(data []byte, reliable bool) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	// Queue the message for the next poll
	t.session.QueueMessage(data)
	return nil
}

// Close closes the transport
func (t *LongPollTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	return nil
}

// TransportType returns the transport protocol type
func (t *LongPollTransport) TransportType() byte {
	return backend.ProtocolWebSocket // Use same wire format as WebSocket
}

// IsClosed returns whether the transport is closed
func (t *LongPollTransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// =============================================================================
// Long Polling Session
// =============================================================================

// LongPollSession manages state for a single Long Polling session.
// A session is created on the first "start" request and persists until
// it times out or the client sends a disconnect.
type LongPollSession struct {
	ID       string // Session ID (returned to client)
	Password string // Session password for auth

	transport  *LongPollTransport
	client     *ClientConn
	projectID  string
	hostname   string
	callbackID string // JSONP callback identifier (cb parameter)

	mu         sync.Mutex
	lastActive time.Time
	closed     bool
	upgraded   bool // True if WS has taken over (LP polls still valid until END_TRANSMISSION)

	// Message queue (messages waiting to be delivered to client)
	messagesMu   sync.Mutex
	messages     [][]byte
	packetNumber int // Monotonically increasing packet number

	// Pending poll (if a client is waiting for messages)
	pollMu      sync.Mutex
	pollWaiter  chan struct{}
	pollTimeout *time.Timer
}

// NewLongPollSession creates a new Long Poll session
func NewLongPollSession(projectID, hostname, callbackID string) *LongPollSession {
	// Generate random session ID (numeric, like Firebase uses)
	idBytes := make([]byte, 4)
	if _, err := rand.Read(idBytes); err != nil {
		panic(err)
	}
	sessionNum := uint32(idBytes[0])<<24 | uint32(idBytes[1])<<16 | uint32(idBytes[2])<<8 | uint32(idBytes[3])

	session := &LongPollSession{
		ID:         strconv.FormatUint(uint64(sessionNum%10000000), 10),
		Password:   generatePassword(10),
		projectID:  projectID,
		hostname:   hostname,
		callbackID: callbackID,
		lastActive: time.Now(),
		messages:   make([][]byte, 0, 32),
	}

	return session
}

// generatePassword generates a random alphanumeric password.
// The 10-char password is the second factor that makes long-poll sessions
// resistant to enumeration; silently zeroing it would defeat that.
func generatePassword(length int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

// SetClient sets the client connection for this session
func (s *LongPollSession) SetClient(client *ClientConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = client
}

// Touch updates the last active time
func (s *LongPollSession) Touch() {
	s.mu.Lock()
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// LastActive returns the last active time
func (s *LongPollSession) LastActive() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActive
}

// IsClosed returns whether the session is closed
func (s *LongPollSession) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close closes the session
func (s *LongPollSession) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	// Close any pending poll waiter
	s.pollMu.Lock()
	if s.pollWaiter != nil {
		close(s.pollWaiter)
		s.pollWaiter = nil
	}
	if s.pollTimeout != nil {
		s.pollTimeout.Stop()
		s.pollTimeout = nil
	}
	s.pollMu.Unlock()

	// Close the client if we have one
	if s.client != nil {
		s.client.Close()
	}

	// Close transport
	if s.transport != nil {
		s.transport.Close()
	}
}

// QueueMessage adds a message to the queue for delivery
func (s *LongPollSession) QueueMessage(data []byte) {
	s.messagesMu.Lock()
	s.messages = append(s.messages, data)
	s.messagesMu.Unlock()

	// Wake up any waiting poll
	s.pollMu.Lock()
	if s.pollWaiter != nil {
		close(s.pollWaiter)
		s.pollWaiter = nil
	}
	s.pollMu.Unlock()
}

// DrainMessages returns all queued messages and clears the queue
func (s *LongPollSession) DrainMessages() ([][]byte, int) {
	s.messagesMu.Lock()
	defer s.messagesMu.Unlock()

	msgs := s.messages
	s.messages = make([][]byte, 0, 32)
	s.packetNumber++

	return msgs, s.packetNumber
}

// WaitForMessages waits for messages with a timeout
// Returns immediately if messages are available, otherwise waits
func (s *LongPollSession) WaitForMessages(timeout time.Duration) ([][]byte, int) {
	// Check if we already have messages
	s.messagesMu.Lock()
	if len(s.messages) > 0 {
		msgs := s.messages
		s.messages = make([][]byte, 0, 32)
		s.packetNumber++
		pn := s.packetNumber
		s.messagesMu.Unlock()
		return msgs, pn
	}
	s.messagesMu.Unlock()

	// No messages - wait for some to arrive or timeout
	s.pollMu.Lock()
	if s.pollWaiter != nil {
		// Another poll is already waiting - shouldn't happen but handle gracefully
		s.pollMu.Unlock()
		return s.DrainMessages()
	}

	waiter := make(chan struct{})
	s.pollWaiter = waiter
	timer := time.NewTimer(timeout)
	s.pollTimeout = timer
	s.pollMu.Unlock()

	// Wait for either messages or timeout
	select {
	case <-waiter:
		// Messages arrived
		timer.Stop()
	case <-timer.C:
		// Timeout - return empty
		s.pollMu.Lock()
		s.pollWaiter = nil
		s.pollTimeout = nil
		s.pollMu.Unlock()
	}

	return s.DrainMessages()
}

// =============================================================================
// Long Polling Pool
// =============================================================================

// LongPollPool manages active Long Polling sessions
type LongPollPool struct {
	server *Server

	mu       sync.RWMutex
	sessions map[string]*LongPollSession // key: session ID

	// Track recently closed sessions so we can send 'close' instead of 'error'
	// This prevents the SDK from hammering us with retries
	closedMu      sync.RWMutex
	closedSessions map[string]time.Time // session ID -> when closed

	idleTimeout time.Duration
}

// NewLongPollPool creates a new Long Poll pool
func NewLongPollPool(server *Server, idleTimeout time.Duration) *LongPollPool {
	pool := &LongPollPool{
		server:         server,
		sessions:       make(map[string]*LongPollSession),
		closedSessions: make(map[string]time.Time),
		idleTimeout:    idleTimeout,
	}

	// Start cleanup goroutine
	go pool.cleanupLoop()

	return pool
}

// CreateSession creates a new Long Poll session
func (p *LongPollPool) CreateSession(projectID, hostname, callbackID string) (*LongPollSession, error) {
	session := NewLongPollSession(projectID, hostname, callbackID)

	// Create transport
	transport := NewLongPollTransport(session)
	session.transport = transport

	// Allocate client ID
	clientID := p.server.allocateClientID()
	if clientID == 0 {
		return nil, fmt.Errorf("too many concurrent connections")
	}

	// Create client connection
	client := newClientConn(p.server, clientID, transport, ProtocolFirebase)
	transport.SetClient(client)
	session.SetClient(client)

	// Set client metadata
	client.projectID = projectID
	client.hostname = hostname

	// Fetch project config
	client.fetchProjectConfig()

	// Register client with server
	p.server.registerClient(client)

	// Start client's write loop
	client.Start()

	// Register session
	p.mu.Lock()
	p.sessions[session.ID] = session
	p.mu.Unlock()

	logger.Debug("LP created session", "session_id", session.ID, "project_id", projectID)

	return session, nil
}

// GetSession retrieves a session by ID
func (p *LongPollPool) GetSession(id string) *LongPollSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.sessions[id]
}

// ValidateSession checks if session ID and password match
// Returns the session even if upgraded (WS took over) - LP polls are still valid until END_TRANSMISSION
func (p *LongPollPool) ValidateSession(id, password string) (*LongPollSession, bool) {
	p.mu.RLock()
	session, ok := p.sessions[id]
	p.mu.RUnlock()

	if !ok || session.Password != password || session.IsClosed() {
		return nil, false
	}

	session.Touch()
	return session, true
}

// IsUpgraded returns true if WS has taken over this session
func (s *LongPollSession) IsUpgraded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upgraded
}

// MarkUpgraded marks the session as upgraded (WS took over)
func (s *LongPollSession) MarkUpgraded() {
	s.mu.Lock()
	s.upgraded = true
	s.mu.Unlock()
}

// GetSessionByFirebaseID finds a session by its Firebase session ID (the "s" parameter from hello)
// Returns nil if not found
func (p *LongPollPool) GetSessionByFirebaseID(firebaseSessionID string) *LongPollSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, session := range p.sessions {
		if session.client != nil &&
			session.client.firebaseState != nil &&
			session.client.firebaseState.SessionID == firebaseSessionID {
			return session
		}
	}
	return nil
}

// RemoveSession removes and closes a session
func (p *LongPollPool) RemoveSession(id string) {
	p.mu.Lock()
	session, ok := p.sessions[id]
	if ok {
		delete(p.sessions, id)
	}
	p.mu.Unlock()

	if session != nil {
		session.Close()
	}

	// Track as recently closed so we can send 'close' instead of 'error'
	p.closedMu.Lock()
	p.closedSessions[id] = time.Now()
	p.closedMu.Unlock()
}

// WasRecentlyClosed checks if a session was closed within the last 30 seconds
func (p *LongPollPool) WasRecentlyClosed(id string) bool {
	p.closedMu.RLock()
	closedAt, ok := p.closedSessions[id]
	p.closedMu.RUnlock()

	if !ok {
		return false
	}
	return time.Since(closedAt) < 30*time.Second
}

// cleanupLoop periodically removes idle sessions
func (p *LongPollPool) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()

		// Clean up idle sessions
		p.mu.Lock()
		for id, session := range p.sessions {
			if now.Sub(session.LastActive()) > p.idleTimeout {
				logger.Debug("LP removing idle session", "session_id", id)
				session.Close()
				delete(p.sessions, id)
				// Track as closed
				p.closedMu.Lock()
				p.closedSessions[id] = now
				p.closedMu.Unlock()
			}
		}
		p.mu.Unlock()

		// Clean up old closed session records (older than 60 seconds)
		p.closedMu.Lock()
		for id, closedAt := range p.closedSessions {
			if now.Sub(closedAt) > 60*time.Second {
				delete(p.closedSessions, id)
			}
		}
		p.closedMu.Unlock()
	}
}

// Close closes all sessions in the pool
func (p *LongPollPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, session := range p.sessions {
		session.Close()
	}
	p.sessions = make(map[string]*LongPollSession)
}

// =============================================================================
// HTTP Handler
// =============================================================================

// handleLongPoll handles Firebase Long Polling requests
// Endpoint: /.lp
func (s *Server) handleLongPoll(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// Extract common parameters
	callbackID := query.Get("cb")
	if callbackID == "" {
		callbackID = "0"
	}

	// Check if this is a start request
	if query.Get("start") == "t" {
		s.handleLongPollStart(w, r, callbackID)
		return
	}

	// Otherwise it's a poll request - need session ID and password
	sessionID := query.Get("id")
	password := query.Get("pw")

	if sessionID == "" || password == "" {
		s.writeLongPollError(w, callbackID, "Missing session credentials")
		return
	}

	session, ok := s.lpPool.ValidateSession(sessionID, password)
	if !ok {
		// Log why validation failed
		s.lpPool.mu.RLock()
		existingSession, exists := s.lpPool.sessions[sessionID]
		s.lpPool.mu.RUnlock()

		if !exists {
			logger.Debug("LP session not found in pool", "session_id", sessionID)
		} else if existingSession.Password != password {
			logger.Debug("LP session password mismatch", "session_id", sessionID)
		} else if existingSession.IsClosed() {
			logger.Debug("LP session is closed", "session_id", sessionID)
		} else {
			logger.Debug("LP session validation failed for unknown reason", "session_id", sessionID)
		}

		// Check if this session was recently closed - send 'close' instead of 'error'
		// This tells the SDK to stop polling gracefully
		if s.lpPool.WasRecentlyClosed(sessionID) {
			logger.Debug("LP session was recently closed, sending close", "session_id", sessionID)
			s.writeLongPollClose(w, callbackID)
			return
		}
		s.writeLongPollError(w, callbackID, "Invalid session")
		return
	}

	logger.Debug("LP valid poll", "session_id", sessionID, "upgraded", session.IsUpgraded(), "closed", session.IsClosed())

	// Check for data being sent (client → server)
	if hasSegmentedData(query) {
		data, err := parseSegmentedData(query)
		if err != nil {
			logger.Debug("LP failed to parse segmented data", "error", err)
		} else {
			for _, msg := range data {
				// Check if this is END_TRANSMISSION - we need to handle it even if upgraded
				if isEndTransmission(msg) {
					logger.Debug("LP received END_TRANSMISSION", "session_id", session.ID)
					// Send END_TRANSMISSION response
					endTx := []byte(`{"t":"c","d":{"t":"n","d":{}}}`)
					session.QueueMessage(endTx)
					// Remove the session from pool - upgrade is complete
					s.lpPool.RemoveSession(sessionID)
					continue
				}

				// For other messages, only forward if session is not upgraded
				// (WS handles messages after upgrade)
				if session.client != nil && !session.IsUpgraded() {
					session.client.OnMessage(msg, true)
				}
			}
		}
	}

	// Check for disconnect frame
	if query.Get("dframe") == "t" {
		logger.Debug("LP received disconnect frame", "session_id", sessionID, "upgraded", session.IsUpgraded())
		// If session is upgraded, WS has taken over - just acknowledge the close
		// Don't remove immediately, let the session timeout naturally
		if session.IsUpgraded() {
			s.writeLongPollClose(w, callbackID)
			return
		}
		s.lpPool.RemoveSession(sessionID)
		s.writeLongPollClose(w, callbackID)
		return
	}

	// This is a poll request - wait for messages
	s.handleLongPollPoll(w, session, callbackID)
}

// handleLongPollStart handles the initial "start" request
func (s *Server) handleLongPollStart(w http.ResponseWriter, r *http.Request, callbackID string) {
	// Extract project ID (and optional database ID) from subdomain or ns parameter
	projectID, subdomainDB := s.extractProjectAndDatabase(r.Host)
	if projectID == "" {
		projectID = r.URL.Query().Get("ns")
	}
	if projectID == "" {
		s.writeLongPollError(w, callbackID, "Missing project ID")
		return
	}

	// Create session
	session, err := s.lpPool.CreateSession(projectID, r.Host, callbackID)
	if err != nil {
		logger.Error("LP failed to create session", "error", err)
		s.writeLongPollError(w, callbackID, "Failed to create session")
		return
	}

	// Pre-set database ID from subdomain if present
	if subdomainDB != "" {
		session.client.databaseID = subdomainDB
	}

	// Generate session ID for Firebase
	firebaseSessionID := generateSessionID()
	session.client.firebaseState.SessionID = firebaseSessionID

	// Create Firebase hello message
	hello := map[string]interface{}{
		"t": "c",
		"d": map[string]interface{}{
			"t": "h",
			"d": map[string]interface{}{
				"ts": time.Now().UnixMilli(),
				"v":  "5",
				"h":  r.Host,
				"s":  firebaseSessionID,
			},
		},
	}
	helloData, _ := sonic.Marshal(hello)

	// Write JSONP response with start command and hello message
	s.writeLongPollStart(w, callbackID, session.ID, session.Password, [][]byte{helloData})
}

// handleLongPollPoll handles a poll request
func (s *Server) handleLongPollPoll(w http.ResponseWriter, session *LongPollSession, callbackID string) {
	// If session is upgraded (WS took over), return empty response immediately
	// The WS is handling all messages now, LP is just kept alive until END_TRANSMISSION
	if session.IsUpgraded() {
		_, packetNum := session.DrainMessages() // Still increment packet number
		s.writeLongPollMessages(w, callbackID, packetNum, nil)
		return
	}

	// Wait for messages (with timeout)
	// Firebase uses ~30s long poll timeout
	messages, packetNum := session.WaitForMessages(30 * time.Second)

	// Write JSONP response with messages
	s.writeLongPollMessages(w, callbackID, packetNum, messages)
}

// =============================================================================
// JSONP Response Formatting
// =============================================================================

// writeLongPollStart writes the initial "start" response in JSONP format
func (s *Server) writeLongPollStart(w http.ResponseWriter, callbackID, sessionID, password string, messages [][]byte) {
	w.Header().Set("Content-Type", "application/javascript")
	w.WriteHeader(http.StatusOK)

	// Write wrapper functions
	fmt.Fprintf(w, "function pLPCommand(c, a1, a2, a3, a4) {\n")
	fmt.Fprintf(w, "parent.window[\"pLPCommand%s\"] && parent.window[\"pLPCommand%s\"](c, a1, a2, a3, a4);\n", callbackID, callbackID)
	fmt.Fprintf(w, "}\n")
	fmt.Fprintf(w, "function pRTLPCB(pN, data) {\n")
	fmt.Fprintf(w, "parent.window[\"pRTLPCB%s\"] && parent.window[\"pRTLPCB%s\"](pN, data);\n", callbackID, callbackID)
	fmt.Fprintf(w, "}\n")

	// Send start command
	fmt.Fprintf(w, "pLPCommand('start','%s','%s');\n", sessionID, password)

	// Send initial messages
	s.writeMessagesAsJSONP(w, 0, messages)
}

// writeLongPollMessages writes messages in JSONP format
func (s *Server) writeLongPollMessages(w http.ResponseWriter, callbackID string, packetNum int, messages [][]byte) {
	w.Header().Set("Content-Type", "application/javascript")
	w.WriteHeader(http.StatusOK)

	// Write wrapper functions (always included)
	fmt.Fprintf(w, "function pLPCommand(c, a1, a2, a3, a4) {\n")
	fmt.Fprintf(w, "parent.window[\"pLPCommand%s\"] && parent.window[\"pLPCommand%s\"](c, a1, a2, a3, a4);\n", callbackID, callbackID)
	fmt.Fprintf(w, "}\n")
	fmt.Fprintf(w, "function pRTLPCB(pN, data) {\n")
	fmt.Fprintf(w, "parent.window[\"pRTLPCB%s\"] && parent.window[\"pRTLPCB%s\"](pN, data);\n", callbackID, callbackID)
	fmt.Fprintf(w, "}\n")

	// Send messages
	s.writeMessagesAsJSONP(w, packetNum, messages)
}

// writeLongPollClose writes a close command in JSONP format
func (s *Server) writeLongPollClose(w http.ResponseWriter, callbackID string) {
	w.Header().Set("Content-Type", "application/javascript")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "function pLPCommand(c, a1, a2, a3, a4) {\n")
	fmt.Fprintf(w, "parent.window[\"pLPCommand%s\"] && parent.window[\"pLPCommand%s\"](c, a1, a2, a3, a4);\n", callbackID, callbackID)
	fmt.Fprintf(w, "}\n")
	fmt.Fprintf(w, "pLPCommand('close');\n")
}

// writeLongPollError writes an error in JSONP format
// Includes a 1 second delay to prevent clients from hammering the server on errors
func (s *Server) writeLongPollError(w http.ResponseWriter, callbackID, message string) {
	// Delay error responses to prevent rapid retries from DDoSing the server
	time.Sleep(1 * time.Second)

	w.Header().Set("Content-Type", "application/javascript")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "function pLPCommand(c, a1, a2, a3, a4) {\n")
	fmt.Fprintf(w, "parent.window[\"pLPCommand%s\"] && parent.window[\"pLPCommand%s\"](c, a1, a2, a3, a4);\n", callbackID, callbackID)
	fmt.Fprintf(w, "}\n")
	fmt.Fprintf(w, "pLPCommand('error','%s');\n", message)
}

// writeMessagesAsJSONP writes messages as a pRTLPCB call
func (s *Server) writeMessagesAsJSONP(w http.ResponseWriter, packetNum int, messages [][]byte) {
	// Build message array - each message is raw JSON, combine into array
	var msgStrings []string
	for _, msg := range messages {
		msgStrings = append(msgStrings, string(msg))
	}

	// Format: pRTLPCB(packetNum, [msg1, msg2, ...])
	if len(msgStrings) == 0 {
		fmt.Fprintf(w, "pRTLPCB(%d,[]);\n", packetNum)
	} else {
		fmt.Fprintf(w, "pRTLPCB(%d,[%s]);\n", packetNum, strings.Join(msgStrings, ","))
	}
}

// =============================================================================
// Segmented Data Parsing
// =============================================================================

// isEndTransmission checks if a message is an END_TRANSMISSION control message
// Format: {"t":"c","d":{"t":"n","d":{}}}
func isEndTransmission(data []byte) bool {
	var msg map[string]interface{}
	if err := sonic.Unmarshal(data, &msg); err != nil {
		return false
	}

	t, _ := msg["t"].(string)
	if t != "c" {
		return false
	}

	d, _ := msg["d"].(map[string]interface{})
	if d == nil {
		return false
	}

	ct, _ := d["t"].(string)
	return ct == "n"
}

// hasSegmentedData checks if the query contains segmented data
func hasSegmentedData(query map[string][]string) bool {
	for key := range query {
		if strings.HasPrefix(key, "d") {
			// Check if it's a data segment (d0, d1, etc.)
			rest := key[1:]
			if _, err := strconv.Atoi(rest); err == nil {
				return true
			}
		}
	}
	return false
}

// segmentInfo holds information about a segment
type segmentInfo struct {
	index int    // Segment index within packet
	total int    // Total segments in packet
	data  string // Base64 data
}

// maxLongPollMessageSize is the maximum size of a decoded Long Polling message.
// Matches the WebSocket/WebTransport limit of 16MB.
const maxLongPollMessageSize = 16 * 1024 * 1024

// parseSegmentedData parses segmented data from URL parameters
// Format: seg0=0&ts0=1&d0=<base64>&seg1=1&ts1=2&d1=<base64>
// Returns parsed messages (one per complete packet)
func parseSegmentedData(query map[string][]string) ([][]byte, error) {
	// Collect all segments
	segments := make(map[int]segmentInfo)

	for key, values := range query {
		if len(values) == 0 {
			continue
		}
		value := values[0]

		// Check for segment index (seg0, seg1, etc.)
		if strings.HasPrefix(key, "seg") {
			idxStr := key[3:]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				continue
			}
			segIdx, err := strconv.Atoi(value)
			if err != nil {
				continue
			}
			if seg, ok := segments[idx]; ok {
				seg.index = segIdx
				segments[idx] = seg
			} else {
				segments[idx] = segmentInfo{index: segIdx}
			}
		}

		// Check for total segments (ts0, ts1, etc.)
		if strings.HasPrefix(key, "ts") {
			idxStr := key[2:]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				continue
			}
			total, err := strconv.Atoi(value)
			if err != nil {
				continue
			}
			if seg, ok := segments[idx]; ok {
				seg.total = total
				segments[idx] = seg
			} else {
				segments[idx] = segmentInfo{total: total}
			}
		}

		// Check for data (d0, d1, etc.)
		if strings.HasPrefix(key, "d") && len(key) > 1 {
			idxStr := key[1:]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				continue
			}
			if seg, ok := segments[idx]; ok {
				seg.data = value
				segments[idx] = seg
			} else {
				segments[idx] = segmentInfo{data: value}
			}
		}
	}

	if len(segments) == 0 {
		return nil, nil
	}

	// Sort segments by their declared index within the packet
	type indexedSegment struct {
		paramIdx int // Parameter index (0, 1, 2...)
		info     segmentInfo
	}
	var sorted []indexedSegment
	for paramIdx, info := range segments {
		sorted = append(sorted, indexedSegment{paramIdx: paramIdx, info: info})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].info.index < sorted[j].info.index
	})

	// Concatenate data from all segments
	// Base64 encoding inflates by ~4/3, so cap the encoded size proportionally
	maxEncodedSize := maxLongPollMessageSize/3*4 + 4 // base64 overhead + padding
	var combined strings.Builder
	for _, seg := range sorted {
		combined.WriteString(seg.info.data)
		if combined.Len() > maxEncodedSize {
			return nil, fmt.Errorf("segmented data too large (%d bytes encoded, max %d)", combined.Len(), maxEncodedSize)
		}
	}

	// Base64 decode
	decoded, err := base64.StdEncoding.DecodeString(combined.String())
	if err != nil {
		// Try URL-safe base64
		decoded, err = base64.URLEncoding.DecodeString(combined.String())
		if err != nil {
			// Try raw base64 (no padding)
			decoded, err = base64.RawStdEncoding.DecodeString(combined.String())
			if err != nil {
				return nil, fmt.Errorf("base64 decode failed: %w", err)
			}
		}
	}

	// Enforce size limit on decoded data (matches WebSocket/WebTransport 16MB limit)
	if len(decoded) > maxLongPollMessageSize {
		return nil, fmt.Errorf("message too large (%d bytes, max %d)", len(decoded), maxLongPollMessageSize)
	}

	// The decoded data is JSON (Firebase message)
	// It could be a single message or multiple messages separated somehow
	// For now, treat as single message
	return [][]byte{decoded}, nil
}

