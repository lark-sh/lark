package proxy

import (
	"testing"

	"github.com/bytedance/sonic"

	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/testutil"
)

// newTestClient creates a ClientConn for testing with required channels initialized
func newTestClient(id uint32, transport ClientTransport, protocol Protocol) *ClientConn {
	c := &ClientConn{
		id:        id,
		transport: transport,
		protocol:  protocol,
		outbox:    make(chan *outboxMessage, 100),
		done:      make(chan struct{}),
	}
	c.state.Store(int32(StateConnected))
	if protocol == ProtocolFirebase {
		c.firebaseState = &FirebaseState{}
	}
	return c
}

func TestClientStateTransitions(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	// Initial state should be Connected
	if client.State() != StateConnected {
		t.Errorf("Initial state: got %v, want %v", client.State(), StateConnected)
	}

	// Transition to Routing
	client.SetState(StateRouting)
	if client.State() != StateRouting {
		t.Errorf("After SetState(Routing): got %v, want %v", client.State(), StateRouting)
	}

	// Transition to Forwarding
	client.SetState(StateForwarding)
	if client.State() != StateForwarding {
		t.Errorf("After SetState(Forwarding): got %v, want %v", client.State(), StateForwarding)
	}

	// Transition to Closing
	client.SetState(StateClosing)
	if client.State() != StateClosing {
		t.Errorf("After SetState(Closing): got %v, want %v", client.State(), StateClosing)
	}
}

func TestClientMessageBuffering(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)
	client.state.Store(int32(StateRouting))

	// Buffer messages while in Routing state
	msg1 := []byte(`{"type":"test1"}`)
	msg2 := []byte(`{"type":"test2"}`)

	client.OnMessage(msg1, true)
	client.OnMessage(msg2, true)

	// Check buffer
	client.bufferMu.Lock()
	defer client.bufferMu.Unlock()

	if len(client.buffer) != 2 {
		t.Fatalf("Buffer length: got %d, want 2", len(client.buffer))
	}

	if string(client.buffer[0]) != string(msg1) {
		t.Errorf("Buffer[0]: got %q, want %q", string(client.buffer[0]), string(msg1))
	}
	if string(client.buffer[1]) != string(msg2) {
		t.Errorf("Buffer[1]: got %q, want %q", string(client.buffer[1]), string(msg2))
	}
}

func TestClientLarkJoinMessageParsing(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	// Set project ID (normally comes from subdomain at connection time)
	client.projectID = "my-project"

	// Create join message
	joinMsg := testutil.LarkJoinMessage("my-project", "my-database")

	// Call handleLarkMessage directly
	client.handleLarkMessage(joinMsg)

	// Check that database ID was extracted
	if client.databaseID != "my-database" {
		t.Errorf("databaseID: got %q, want %q", client.databaseID, "my-database")
	}

	// Check that join request ID was extracted
	if client.joinRequestID == "" {
		t.Error("joinRequestID should be set")
	}

	// Check that message was buffered
	client.bufferMu.Lock()
	if len(client.buffer) != 1 {
		t.Errorf("Buffer length: got %d, want 1", len(client.buffer))
	}
	client.bufferMu.Unlock()
}

func TestClientLarkInvalidJSON(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	// Send invalid JSON
	client.handleLarkMessage([]byte("not valid json"))

	// Client should be closed
	if !transport.IsClosed() {
		t.Error("Transport should be closed after invalid JSON")
	}
}

func TestClientLarkMissingFields(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	// Send join with invalid path format (no slash separator)
	msg := testutil.MustMarshalJSON(map[string]interface{}{
		"o": "j",
		"d": "my-project-only", // missing /database
		"r": "r1",
	})

	client.handleLarkMessage(msg)

	// Should have sent error
	if transport.MessageCount() == 0 {
		t.Error("Should have sent error message")
	}

	// Check error message
	lastMsg, _ := transport.LastMessage()
	var parsed map[string]interface{}
	sonic.Unmarshal(lastMsg, &parsed)
	if parsed["type"] != "error" {
		t.Errorf("Expected error message, got %v", parsed)
	}
}

func TestClientFirebaseKeepalive(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Send keepalive
	client.handleFirebaseMessage([]byte("0"))

	// Should respond with keepalive
	if transport.MessageCount() != 1 {
		t.Fatalf("Should have sent 1 message, got %d", transport.MessageCount())
	}

	lastMsg, _ := transport.LastMessage()
	if string(lastMsg) != "0" {
		t.Errorf("Expected keepalive response '0', got %q", string(lastMsg))
	}

	// Buffer should be empty (keepalive not forwarded)
	client.bufferMu.Lock()
	if len(client.buffer) != 0 {
		t.Errorf("Keepalive should not be buffered, got %d messages", len(client.buffer))
	}
	client.bufferMu.Unlock()
}

func TestClientFirebaseAuthMessage(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Send auth message
	authMsg := testutil.FirebaseAuthMessage(1, "test-token")
	client.handleFirebaseMessage(authMsg)

	// Auth token should be extracted
	if client.authToken != "test-token" {
		t.Errorf("authToken: got %q, want %q", client.authToken, "test-token")
	}

	// Auth message should be buffered (backend still needs it)
	client.bufferMu.Lock()
	if len(client.buffer) != 1 {
		t.Errorf("Auth message should be buffered, got %d messages", len(client.buffer))
	}
	client.bufferMu.Unlock()

	// Note: Auth response is sent asynchronously after project config is ready
	// We can't easily test the async response here without a full server setup
}

func TestClientFirebaseStatsMessage(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Send stats message
	statsMsg := testutil.FirebaseStatsMessage(2)
	client.handleFirebaseMessage(statsMsg)

	// Should respond with OK
	if transport.MessageCount() != 1 {
		t.Fatalf("Should have sent 1 message, got %d", transport.MessageCount())
	}

	// Parse response
	lastMsg, _ := transport.LastMessage()
	var response map[string]interface{}
	if err := sonic.Unmarshal(lastMsg, &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	d := response["d"].(map[string]interface{})
	if d["r"] != float64(2) {
		t.Errorf("Request ID: got %v, want 2", d["r"])
	}

	b := d["b"].(map[string]interface{})
	if b["s"] != "ok" {
		t.Errorf("Status: got %v, want 'ok'", b["s"])
	}

	// Stats message should be buffered
	client.bufferMu.Lock()
	if len(client.buffer) != 1 {
		t.Errorf("Stats message should be buffered")
	}
	client.bufferMu.Unlock()
}

func TestClientFirebaseSwitchAckMessage(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Send SWITCH_ACK message (sent by client after WS connects during LP→WS upgrade)
	switchAckMsg := testutil.FirebaseSwitchAckMessage()
	client.handleFirebaseMessage(switchAckMsg)

	// SWITCH_ACK should NOT be buffered (it's a local control message)
	client.bufferMu.Lock()
	bufLen := len(client.buffer)
	client.bufferMu.Unlock()

	if bufLen != 0 {
		t.Errorf("SWITCH_ACK should not be buffered, got %d messages in buffer", bufLen)
	}

	// No response expected for SWITCH_ACK
	if transport.MessageCount() != 0 {
		t.Errorf("SWITCH_ACK should not send response, got %d messages", transport.MessageCount())
	}
}

func TestClientFirebaseEndTransmissionMessage(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Send END_TRANSMISSION message (sent by client on LP after switch to WS)
	endTxMsg := testutil.FirebaseEndTransmissionMessage()
	client.handleFirebaseMessage(endTxMsg)

	// END_TRANSMISSION should NOT be buffered (it's a local control message)
	client.bufferMu.Lock()
	bufLen := len(client.buffer)
	client.bufferMu.Unlock()

	if bufLen != 0 {
		t.Errorf("END_TRANSMISSION should not be buffered, got %d messages in buffer", bufLen)
	}

	// Server should respond with END_TRANSMISSION
	if transport.MessageCount() != 1 {
		t.Fatalf("Should have sent END_TRANSMISSION response, got %d messages", transport.MessageCount())
	}

	// Parse response
	lastMsg, _ := transport.LastMessage()
	var response map[string]interface{}
	if err := sonic.Unmarshal(lastMsg, &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Should be control message type "c"
	if response["t"] != "c" {
		t.Errorf("Response type: got %v, want 'c'", response["t"])
	}

	d := response["d"].(map[string]interface{})
	// Should be END_TRANSMISSION subtype "n"
	if d["t"] != "n" {
		t.Errorf("Control subtype: got %v, want 'n' (END_TRANSMISSION)", d["t"])
	}
}

func TestClientSendError(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
	}{
		{"lark", ProtocolLark},
		{"firebase", ProtocolFirebase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := NewMockTransport(backend.ProtocolWebSocket)
			client := newTestClient(1, transport, tt.protocol)

			client.sendError("test error message")

			if transport.MessageCount() != 1 {
				t.Fatalf("Should have sent 1 message, got %d", transport.MessageCount())
			}

			lastMsg, _ := transport.LastMessage()
			var parsed map[string]interface{}
			if err := sonic.Unmarshal(lastMsg, &parsed); err != nil {
				t.Fatalf("Failed to parse error message: %v", err)
			}

			if tt.protocol == ProtocolLark {
				if parsed["type"] != "error" {
					t.Errorf("Expected type 'error', got %v", parsed["type"])
				}
				if parsed["error"] != "test error message" {
					t.Errorf("Expected error 'test error message', got %v", parsed["error"])
				}
			} else {
				// Firebase error format
				if parsed["t"] != "c" {
					t.Errorf("Expected type 'c', got %v", parsed["t"])
				}
				d := parsed["d"].(map[string]interface{})
				if d["t"] != "e" {
					t.Errorf("Expected d.t 'e', got %v", d["t"])
				}
				if d["d"] != "test error message" {
					t.Errorf("Expected d.d 'test error message', got %v", d["d"])
				}
			}
		})
	}
}

func TestClientClose(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)
	client.state.Store(int32(StateForwarding))

	// Close should transition to Closing
	client.Close()

	if client.State() != StateClosing {
		t.Errorf("State after close: got %v, want %v", client.State(), StateClosing)
	}

	// Transport should be closed
	if !transport.IsClosed() {
		t.Error("Transport should be closed")
	}

	// Double close should be safe
	client.Close()
	// No panic = success
}

func TestClientCloseOnlyOnce(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	closeCount := 0

	// Custom transport that counts closes
	countingTransport := &countingCloseTransport{
		MockTransport: transport,
		closeCount:    &closeCount,
	}

	client := newTestClient(1, countingTransport, ProtocolLark)

	// Multiple closes should only close transport once
	client.Close()
	client.Close()
	client.Close()

	if closeCount != 1 {
		t.Errorf("Close should only be called once, got %d", closeCount)
	}
}

type countingCloseTransport struct {
	*MockTransport
	closeCount *int
}

func (c *countingCloseTransport) Close() error {
	*c.closeCount++
	return c.MockTransport.Close()
}

func TestClientID(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(12345, transport, ProtocolLark)

	if client.ID() != 12345 {
		t.Errorf("ID: got %d, want 12345", client.ID())
	}
}

func TestNewClientConn(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	if client.State() != StateConnected {
		t.Errorf("Initial state should be Connected")
	}
	if client.protocol != ProtocolLark {
		t.Errorf("Protocol should be Lark")
	}
}

func TestClientFirebaseState(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Firebase client should have firebase state (created by newTestClient)
	if client.firebaseState == nil {
		t.Error("Firebase client should have firebaseState")
	}
}

func TestClientOnMessageInClosingState(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)
	client.state.Store(int32(StateClosing))

	// Messages in Closing state should be ignored
	client.OnMessage([]byte("test"), true)

	// No messages should be sent or buffered
	if transport.MessageCount() != 0 {
		t.Error("No messages should be sent in Closing state")
	}

	client.bufferMu.Lock()
	if len(client.buffer) != 0 {
		t.Error("No messages should be buffered in Closing state")
	}
	client.bufferMu.Unlock()
}

func TestSendFirebaseOK(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Test with data
	client.sendFirebaseOK(5, map[string]interface{}{"foo": "bar"})

	if transport.MessageCount() != 1 {
		t.Fatalf("Should have sent 1 message")
	}

	lastMsg, _ := transport.LastMessage()
	var response map[string]interface{}
	sonic.Unmarshal(lastMsg, &response)

	d := response["d"].(map[string]interface{})
	if d["r"] != float64(5) {
		t.Errorf("Request ID: got %v, want 5", d["r"])
	}

	b := d["b"].(map[string]interface{})
	bData := b["d"].(map[string]interface{})
	if bData["foo"] != "bar" {
		t.Errorf("Data: got %v, want foo=bar", bData)
	}
}

func TestSendFirebaseOKWithString(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Test with empty string data (like stats response)
	client.sendFirebaseOK(10, "")

	if transport.MessageCount() != 1 {
		t.Fatalf("Should have sent 1 message")
	}

	lastMsg, _ := transport.LastMessage()
	var response map[string]interface{}
	sonic.Unmarshal(lastMsg, &response)

	d := response["d"].(map[string]interface{})
	b := d["b"].(map[string]interface{})
	if b["d"] != "" {
		t.Errorf("Data should be empty string, got %v", b["d"])
	}
}

func TestClientLarkJoinMessage(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)
	client.projectID = "my-project" // Set from subdomain

	// Create join message (tokens now come in separate auth messages)
	joinMsg := testutil.MustMarshalJSON(map[string]interface{}{
		"o": "j",
		"d": "my-project/my-database",
		"r": "r1",
	})

	client.handleLarkMessage(joinMsg)

	// Check that database ID was extracted
	if client.databaseID != "my-database" {
		t.Errorf("databaseID: got %q, want %q", client.databaseID, "my-database")
	}

	// Check that join request ID was extracted
	if client.joinRequestID != "r1" {
		t.Errorf("joinRequestID: got %q, want %q", client.joinRequestID, "r1")
	}
}

func TestClientLarkAuthMessage(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	// Send auth message before join
	authMsg := testutil.MustMarshalJSON(map[string]interface{}{
		"o": "au",
		"t": "late-auth-token",
		"r": "r1",
	})

	client.handleLarkMessage(authMsg)

	// Check that token was extracted
	if client.authToken != "late-auth-token" {
		t.Errorf("authToken: got %q, want %q", client.authToken, "late-auth-token")
	}
}

func TestClientFirebaseAuthMessageExtractsToken(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	// Send auth message with cred
	authMsg := testutil.MustMarshalJSON(map[string]interface{}{
		"t": "d",
		"d": map[string]interface{}{
			"r": 1,
			"a": "auth",
			"b": map[string]interface{}{
				"cred": "firebase-auth-token",
			},
		},
	})

	client.handleFirebaseMessage(authMsg)

	// Check that token was extracted
	if client.authToken != "firebase-auth-token" {
		t.Errorf("authToken: got %q, want %q", client.authToken, "firebase-auth-token")
	}
}

func TestClientIsAuthMessageLark(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolLark)

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "auth message",
			data:     testutil.MustMarshalJSON(map[string]interface{}{"o": "au", "t": "token"}),
			expected: true,
		},
		{
			name:     "join message",
			data:     testutil.MustMarshalJSON(map[string]interface{}{"o": "j", "d": "proj/db"}),
			expected: false,
		},
		{
			name:     "set message",
			data:     testutil.MustMarshalJSON(map[string]interface{}{"o": "s", "p": "/path", "v": "data"}),
			expected: false,
		},
		{
			name:     "invalid json",
			data:     []byte("not json"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.isAuthMessage(tt.data)
			if result != tt.expected {
				t.Errorf("isAuthMessage: got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestClientIsAuthMessageFirebase(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name: "auth message",
			data: testutil.MustMarshalJSON(map[string]interface{}{
				"t": "d",
				"d": map[string]interface{}{"a": "auth", "r": 1, "b": map[string]interface{}{}},
			}),
			expected: true,
		},
		{
			name: "gauth message",
			data: testutil.MustMarshalJSON(map[string]interface{}{
				"t": "d",
				"d": map[string]interface{}{"a": "gauth", "r": 1, "b": map[string]interface{}{}},
			}),
			expected: true,
		},
		{
			name: "query message",
			data: testutil.MustMarshalJSON(map[string]interface{}{
				"t": "d",
				"d": map[string]interface{}{"a": "q", "r": 1, "b": map[string]interface{}{}},
			}),
			expected: false,
		},
		{
			name: "control message",
			data: testutil.MustMarshalJSON(map[string]interface{}{
				"t": "c",
				"d": map[string]interface{}{},
			}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.isAuthMessage(tt.data)
			if result != tt.expected {
				t.Errorf("isAuthMessage: got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSendFirebaseAuthError(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)

	client.sendFirebaseAuthError(5, "token expired")

	if transport.MessageCount() != 1 {
		t.Fatalf("Should have sent 1 message, got %d", transport.MessageCount())
	}

	lastMsg, _ := transport.LastMessage()
	var response map[string]interface{}
	if err := sonic.Unmarshal(lastMsg, &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Check structure
	if response["t"] != "d" {
		t.Errorf("Response type: got %v, want 'd'", response["t"])
	}

	d := response["d"].(map[string]interface{})
	if d["r"] != float64(5) {
		t.Errorf("Request ID: got %v, want 5", d["r"])
	}

	b := d["b"].(map[string]interface{})
	if b["s"] != "error" {
		t.Errorf("Status: got %v, want 'error'", b["s"])
	}
	if b["d"] != "token expired" {
		t.Errorf("Error message: got %v, want 'token expired'", b["d"])
	}
}
