package proxy

import (
	"encoding/base64"
	"testing"
	"time"
)

// TestNewLongPollSession tests session creation
func TestNewLongPollSession(t *testing.T) {
	session := NewLongPollSession("test-project", "test.larkdb.net", "123")

	if session.ID == "" {
		t.Error("Session ID should not be empty")
	}
	if session.Password == "" {
		t.Error("Session Password should not be empty")
	}
	if len(session.Password) != 10 {
		t.Errorf("Password should be 10 chars, got %d", len(session.Password))
	}
	if session.projectID != "test-project" {
		t.Errorf("ProjectID: got %q, want %q", session.projectID, "test-project")
	}
	if session.hostname != "test.larkdb.net" {
		t.Errorf("Hostname: got %q, want %q", session.hostname, "test.larkdb.net")
	}
	if session.callbackID != "123" {
		t.Errorf("CallbackID: got %q, want %q", session.callbackID, "123")
	}
}

// TestLongPollSessionQueueMessage tests message queueing
func TestLongPollSessionQueueMessage(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")

	// Queue some messages
	session.QueueMessage([]byte(`{"msg": 1}`))
	session.QueueMessage([]byte(`{"msg": 2}`))
	session.QueueMessage([]byte(`{"msg": 3}`))

	// Drain and check
	msgs, pn := session.DrainMessages()
	if len(msgs) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(msgs))
	}
	if pn != 1 {
		t.Errorf("Packet number should be 1, got %d", pn)
	}

	// Drain again - should be empty
	msgs2, pn2 := session.DrainMessages()
	if len(msgs2) != 0 {
		t.Errorf("Expected 0 messages after drain, got %d", len(msgs2))
	}
	if pn2 != 2 {
		t.Errorf("Packet number should be 2, got %d", pn2)
	}
}

// TestLongPollSessionWaitForMessages tests the wait mechanism
func TestLongPollSessionWaitForMessages(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")

	// Test immediate return when messages exist
	session.QueueMessage([]byte(`{"ready": true}`))
	msgs, _ := session.WaitForMessages(1 * time.Second)
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message, got %d", len(msgs))
	}

	// Test timeout when no messages
	start := time.Now()
	msgs, _ = session.WaitForMessages(100 * time.Millisecond)
	elapsed := time.Since(start)
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages on timeout, got %d", len(msgs))
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("Should have waited ~100ms, only waited %v", elapsed)
	}

	// Test wakeup when message arrives
	go func() {
		time.Sleep(50 * time.Millisecond)
		session.QueueMessage([]byte(`{"arrived": true}`))
	}()
	start = time.Now()
	msgs, _ = session.WaitForMessages(1 * time.Second)
	elapsed = time.Since(start)
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message, got %d", len(msgs))
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Should have woken up after ~50ms, took %v", elapsed)
	}
}

// TestLongPollSessionClose tests session closure
func TestLongPollSessionClose(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")

	if session.IsClosed() {
		t.Error("Session should not be closed initially")
	}

	session.Close()

	if !session.IsClosed() {
		t.Error("Session should be closed after Close()")
	}

	// Double close should be safe
	session.Close()
}

// TestLongPollSessionTouch tests the activity timestamp
func TestLongPollSessionTouch(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")
	initial := session.LastActive()

	time.Sleep(10 * time.Millisecond)
	session.Touch()

	if !session.LastActive().After(initial) {
		t.Error("LastActive should be updated after Touch()")
	}
}

// TestGeneratePassword tests password generation
func TestGeneratePassword(t *testing.T) {
	pw1 := generatePassword(10)
	pw2 := generatePassword(10)

	if len(pw1) != 10 {
		t.Errorf("Password length should be 10, got %d", len(pw1))
	}
	if pw1 == pw2 {
		t.Error("Passwords should be different")
	}

	// Check alphanumeric
	for _, c := range pw1 {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			t.Errorf("Password contains non-alphanumeric: %c", c)
		}
	}
}

// TestHasSegmentedData tests detection of segmented data
func TestHasSegmentedData(t *testing.T) {
	tests := []struct {
		name  string
		query map[string][]string
		want  bool
	}{
		{
			name:  "no data",
			query: map[string][]string{"id": {"123"}, "pw": {"abc"}},
			want:  false,
		},
		{
			name:  "has d0",
			query: map[string][]string{"d0": {"abc"}},
			want:  true,
		},
		{
			name:  "has d1",
			query: map[string][]string{"d1": {"abc"}, "seg1": {"0"}},
			want:  true,
		},
		{
			name:  "download param not data",
			query: map[string][]string{"download": {"file.json"}},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasSegmentedData(tt.query)
			if got != tt.want {
				t.Errorf("hasSegmentedData() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseSegmentedData tests parsing of segmented base64 data
func TestParseSegmentedData(t *testing.T) {
	// Create a test message
	testMsg := `{"t":"d","d":{"r":1,"a":"auth"}}`
	encoded := base64.StdEncoding.EncodeToString([]byte(testMsg))

	tests := []struct {
		name    string
		query   map[string][]string
		wantLen int
		wantErr bool
	}{
		{
			name:    "no segments",
			query:   map[string][]string{"id": {"123"}},
			wantLen: 0,
			wantErr: false,
		},
		{
			name: "single segment",
			query: map[string][]string{
				"seg0": {"0"},
				"ts0":  {"1"},
				"d0":   {encoded},
			},
			wantLen: 1,
			wantErr: false,
		},
		{
			name: "multiple segments",
			query: map[string][]string{
				"seg0": {"0"},
				"ts0":  {"2"},
				"d0":   {encoded[:len(encoded)/2]},
				"seg1": {"1"},
				"ts1":  {"2"},
				"d1":   {encoded[len(encoded)/2:]},
			},
			wantLen: 1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgs, err := parseSegmentedData(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSegmentedData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(msgs) != tt.wantLen {
				t.Errorf("parseSegmentedData() len = %d, want %d", len(msgs), tt.wantLen)
			}
		})
	}
}

// TestLongPollTransportSend tests transport send queues to session
func TestLongPollTransportSend(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")
	transport := NewLongPollTransport(session)

	// Send data
	err := transport.Send([]byte(`{"test": true}`), true)
	if err != nil {
		t.Errorf("Send() error = %v", err)
	}

	// Check it was queued
	msgs, _ := session.DrainMessages()
	if len(msgs) != 1 {
		t.Errorf("Expected 1 queued message, got %d", len(msgs))
	}
}

// TestLongPollTransportClose tests transport closure
func TestLongPollTransportClose(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")
	transport := NewLongPollTransport(session)

	if transport.IsClosed() {
		t.Error("Transport should not be closed initially")
	}

	err := transport.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if !transport.IsClosed() {
		t.Error("Transport should be closed after Close()")
	}

	// Send after close should not error (silently dropped)
	err = transport.Send([]byte(`{"test": true}`), true)
	if err != nil {
		t.Errorf("Send after close should not error, got %v", err)
	}
}

// TestGetSessionByFirebaseID tests looking up sessions by Firebase session ID
func TestGetSessionByFirebaseID(t *testing.T) {
	// Create a mock pool (can't use real server in unit tests)
	pool := &LongPollPool{
		sessions: make(map[string]*LongPollSession),
	}

	// Create a session with a mock client
	session := NewLongPollSession("test-project", "test.host", "cb")
	session.client = &ClientConn{
		firebaseState: &FirebaseState{
			SessionID: "firebase-session-123",
		},
	}
	pool.sessions[session.ID] = session

	// Should find session by Firebase ID
	found := pool.GetSessionByFirebaseID("firebase-session-123")
	if found == nil {
		t.Error("Should find session by Firebase ID")
	}
	if found != session {
		t.Error("Should return the correct session")
	}

	// Should not find non-existent Firebase ID
	notFound := pool.GetSessionByFirebaseID("non-existent")
	if notFound != nil {
		t.Error("Should not find non-existent Firebase ID")
	}
}

// TestLongPollSessionUpgrade tests the session upgrade flow (LP → WS)
func TestLongPollSessionUpgrade(t *testing.T) {
	session := NewLongPollSession("test", "host", "cb")

	// Simulate client attached
	session.client = &ClientConn{
		id: 123,
		firebaseState: &FirebaseState{
			SessionID: "firebase-123",
		},
	}

	// Simulate upgrade: detach client and mark closed
	session.mu.Lock()
	session.closed = true
	client := session.client
	session.client = nil
	session.mu.Unlock()

	// Session should be closed
	if !session.IsClosed() {
		t.Error("Session should be marked closed after upgrade")
	}

	// Client should still be valid (not closed by session)
	if client == nil {
		t.Error("Client should be preserved during upgrade")
	}
	if client.id != 123 {
		t.Errorf("Client ID should be 123, got %d", client.id)
	}
}
