package backend

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

// TestServerAuthMACKnownAnswer locks the HELLO_AUTH HMAC to a fixed vector so
// the Go edge and Rust server can't silently drift apart (the same vector is
// asserted in the server's proxy.rs test). HMAC-SHA256(key, nonce) where
// key="lark-test-secret" and nonce = bytes 0..31.
func TestServerAuthMACKnownAnswer(t *testing.T) {
	var nonce [32]byte
	for i := range nonce {
		nonce[i] = byte(i)
	}
	got := hex.EncodeToString(ServerAuthMAC("lark-test-secret", nonce))
	const want = "d1e6900018c7d50930190b1577cc590f0821354b51afd79df6935cd08a82acbe"
	if got != want {
		t.Errorf("ServerAuthMAC mismatch:\n got  %s\n want %s", got, want)
	}
}

// TestHelloAckNonceRoundTrip ensures ReadHelloAck recovers the 32-byte nonce
// from a server-shaped HELLO_ACK frame.
func TestHelloAckNonceRoundTrip(t *testing.T) {
	var nonce [32]byte
	for i := range nonce {
		nonce[i] = byte(255 - i)
	}
	frame := make([]byte, 41)
	binary.BigEndian.PutUint32(frame[0:4], 37)
	frame[4] = MsgTypeHelloAck
	frame[5] = 2 // coreID
	frame[6] = 8 // nrCores
	binary.BigEndian.PutUint16(frame[7:9], 1)
	copy(frame[9:41], nonce[:])

	ack, err := ReadHelloAck(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("ReadHelloAck: %v", err)
	}
	if ack.CoreID != 2 || ack.NrCores != 8 || ack.ServerVersion != 1 {
		t.Errorf("header fields wrong: %+v", ack)
	}
	if ack.Nonce != nonce {
		t.Errorf("nonce not preserved: got %x", ack.Nonce)
	}
}

func TestEncodeDecodeConnectPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload *ConnectPayload
	}{
		{
			name: "websocket basic",
			payload: &ConnectPayload{
				Protocol:   ProtocolWebSocket,
				ProjectID:  "my-project",
				DatabaseID: "my-database",
			},
		},
		{
			name: "webtransport basic",
			payload: &ConnectPayload{
				Protocol:   ProtocolWebTransport,
				ProjectID:  "project-123",
				DatabaseID: "db-456",
			},
		},
		{
			name: "with metadata",
			payload: &ConnectPayload{
				Protocol:   ProtocolWebSocket,
				ProjectID:  "test-project",
				DatabaseID: "test-db",
				Metadata:   []byte(`{"ip":"192.168.1.1","ua":"test-client"}`),
			},
		},
		{
			name: "empty metadata",
			payload: &ConnectPayload{
				Protocol:   ProtocolWebSocket,
				ProjectID:  "proj",
				DatabaseID: "db",
				Metadata:   nil,
			},
		},
		{
			name: "long ids",
			payload: &ConnectPayload{
				Protocol:   ProtocolWebSocket,
				ProjectID:  "this-is-a-really-long-project-id-that-might-be-used-in-some-cases",
				DatabaseID: "and-this-is-also-a-really-long-database-id",
			},
		},
		{
			name: "single char ids",
			payload: &ConnectPayload{
				Protocol:   ProtocolWebTransport,
				ProjectID:  "a",
				DatabaseID: "b",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeConnectPayload(tt.payload)
			if err != nil {
				t.Fatalf("EncodeConnectPayload failed: %v", err)
			}

			// Decode
			decoded, err := DecodeConnectPayload(encoded)
			if err != nil {
				t.Fatalf("DecodeConnectPayload failed: %v", err)
			}

			// Verify
			if decoded.Protocol != tt.payload.Protocol {
				t.Errorf("Protocol: got %d, want %d", decoded.Protocol, tt.payload.Protocol)
			}
			if decoded.ProjectID != tt.payload.ProjectID {
				t.Errorf("ProjectID: got %q, want %q", decoded.ProjectID, tt.payload.ProjectID)
			}
			if decoded.DatabaseID != tt.payload.DatabaseID {
				t.Errorf("DatabaseID: got %q, want %q", decoded.DatabaseID, tt.payload.DatabaseID)
			}
			if !bytes.Equal(decoded.Metadata, tt.payload.Metadata) {
				t.Errorf("Metadata: got %v, want %v", decoded.Metadata, tt.payload.Metadata)
			}
		})
	}
}

func TestDecodeConnectPayloadErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty",
			data: []byte{},
		},
		{
			name: "too short",
			data: []byte{0x00, 0x01, 0x02},
		},
		{
			name: "project length overflow",
			data: []byte{0x00, 0xFF, 0x00}, // project length 255, but only 1 more byte
		},
		{
			name: "database length overflow",
			data: []byte{0x00, 0x01, 'a', 0xFF}, // db length 255, but nothing after
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeConnectPayload(tt.data)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestEncodeDecodeDataPayload(t *testing.T) {
	tests := []struct {
		name  string
		flags byte
		data  []byte
	}{
		{
			name:  "reliable empty",
			flags: FlagReliable,
			data:  []byte{},
		},
		{
			name:  "unreliable empty",
			flags: FlagUnreliable,
			data:  []byte{},
		},
		{
			name:  "reliable with data",
			flags: FlagReliable,
			data:  []byte("hello world"),
		},
		{
			name:  "unreliable with data",
			flags: FlagUnreliable,
			data:  []byte(`{"type":"update","data":{"x":100,"y":200}}`),
		},
		{
			name:  "large payload",
			flags: FlagReliable,
			data:  bytes.Repeat([]byte("x"), 1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeDataPayload(tt.flags, tt.data)

			// Decode
			decoded, err := DecodeDataPayload(encoded)
			if err != nil {
				t.Fatalf("DecodeDataPayload failed: %v", err)
			}

			// Verify
			if decoded.Flags != tt.flags {
				t.Errorf("Flags: got %d, want %d", decoded.Flags, tt.flags)
			}
			if !bytes.Equal(decoded.Data, tt.data) {
				t.Errorf("Data: got %v, want %v", decoded.Data, tt.data)
			}
		})
	}
}

func TestDecodeDataPayloadErrors(t *testing.T) {
	// Empty data should error
	_, err := DecodeDataPayload([]byte{})
	if err == nil {
		t.Error("expected error for empty data, got nil")
	}
}

func TestWriteReadMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
	}{
		{
			name: "connect message",
			msg: &Message{
				Type:     MsgTypeConnect,
				ClientID: 12345,
				Payload:  []byte("test payload"),
			},
		},
		{
			name: "data message",
			msg: &Message{
				Type:     MsgTypeData,
				ClientID: 67890,
				Payload:  []byte(`{"type":"join","room":"lobby"}`),
			},
		},
		{
			name: "disconnect message",
			msg: &Message{
				Type:     MsgTypeDisconnect,
				ClientID: 11111,
				Payload:  []byte{DisconnectClean},
			},
		},
		{
			name: "empty payload",
			msg: &Message{
				Type:     MsgTypeClose,
				ClientID: 22222,
				Payload:  nil,
			},
		},
		{
			name: "max client id",
			msg: &Message{
				Type:     MsgTypeData,
				ClientID: 0xFFFFFFFF,
				Payload:  []byte("data"),
			},
		},
		{
			name: "zero client id",
			msg: &Message{
				Type:     MsgTypeData,
				ClientID: 0,
				Payload:  []byte("data"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write to buffer
			var buf bytes.Buffer
			err := WriteMessage(&buf, tt.msg)
			if err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			// Read from buffer
			read, err := ReadMessage(&buf)
			if err != nil {
				t.Fatalf("ReadMessage failed: %v", err)
			}

			// Verify
			if read.Type != tt.msg.Type {
				t.Errorf("Type: got %d, want %d", read.Type, tt.msg.Type)
			}
			if read.ClientID != tt.msg.ClientID {
				t.Errorf("ClientID: got %d, want %d", read.ClientID, tt.msg.ClientID)
			}
			if !bytes.Equal(read.Payload, tt.msg.Payload) {
				t.Errorf("Payload: got %v, want %v", read.Payload, tt.msg.Payload)
			}
		})
	}
}

func TestWriteMessageTooLarge(t *testing.T) {
	msg := &Message{
		Type:     MsgTypeData,
		ClientID: 1,
		Payload:  make([]byte, MaxMessageSize+1),
	}

	var buf bytes.Buffer
	err := WriteMessage(&buf, msg)
	if err != ErrMessageTooLarge {
		t.Errorf("expected ErrMessageTooLarge, got %v", err)
	}
}

func TestReadMessageErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name:    "message too short header",
			data:    []byte{0x00, 0x00, 0x00, 0x04}, // length = 4, too short for type+clientID
			wantErr: ErrInvalidMessage,
		},
		{
			name:    "message too large",
			data:    []byte{0x10, 0x00, 0x00, 0x06}, // length = 268,435,462 > MaxMessageSize+5
			wantErr: ErrMessageTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.data)
			_, err := ReadMessage(buf)
			if err != tt.wantErr {
				t.Errorf("got error %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestMultipleMessages(t *testing.T) {
	// Write multiple messages to same buffer
	var buf bytes.Buffer

	messages := []*Message{
		{Type: MsgTypeConnect, ClientID: 1, Payload: []byte("msg1")},
		{Type: MsgTypeData, ClientID: 2, Payload: []byte("msg2")},
		{Type: MsgTypeDisconnect, ClientID: 3, Payload: []byte{DisconnectClean}},
	}

	for _, msg := range messages {
		if err := WriteMessage(&buf, msg); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}
	}

	// Read them back
	for i, want := range messages {
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage %d failed: %v", i, err)
		}
		if got.Type != want.Type || got.ClientID != want.ClientID || !bytes.Equal(got.Payload, want.Payload) {
			t.Errorf("Message %d mismatch: got %+v, want %+v", i, got, want)
		}
	}
}

func TestProtocolConstants(t *testing.T) {
	// Verify protocol constants are distinct
	if ProtocolWebSocket == ProtocolWebTransport {
		t.Error("Protocol constants should be distinct")
	}

	// Verify proxy->backend message types are distinct
	proxyToBackend := []byte{MsgTypeConnect, MsgTypeData, MsgTypeDisconnect, MsgTypeAuthChanged}
	seen := make(map[byte]bool)
	for _, mt := range proxyToBackend {
		if seen[mt] {
			t.Errorf("Duplicate proxy->backend message type: %d", mt)
		}
		seen[mt] = true
	}

	// Verify backend->proxy message types are distinct
	backendToProxy := []byte{MsgTypeSendData, MsgTypeClose}
	seen = make(map[byte]bool)
	for _, mt := range backendToProxy {
		if seen[mt] {
			t.Errorf("Duplicate backend->proxy message type: %d", mt)
		}
		seen[mt] = true
	}

	// Verify flag constants
	if FlagReliable == FlagUnreliable {
		t.Error("Flag constants should be distinct")
	}
}

func TestEncodeDecodeAuthPayload(t *testing.T) {
	tests := []struct {
		name string
		auth *AuthPayload
	}{
		{
			name: "anonymous",
			auth: &AuthPayload{
				UID:      "",
				Provider: "anonymous",
				Claims:   nil,
			},
		},
		{
			name: "authenticated user",
			auth: &AuthPayload{
				UID:      "user-123",
				Provider: "google",
				Claims:   map[string]any{"role": "admin", "level": float64(5)}, // JSON numbers are float64
			},
		},
		{
			name: "admin user",
			auth: &AuthPayload{
				UID:         "admin-user",
				Provider:    "coordinator",
				Claims:      map[string]any{"isAdmin": true},
				IsTrueAdmin: true,
			},
		},
		{
			name: "empty claims",
			auth: &AuthPayload{
				UID:      "user",
				Provider: "custom",
				Claims:   map[string]any{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeAuthPayload(tt.auth)
			if err != nil {
				t.Fatalf("EncodeAuthPayload failed: %v", err)
			}

			// Decode
			decoded, err := DecodeAuthPayload(encoded)
			if err != nil {
				t.Fatalf("DecodeAuthPayload failed: %v", err)
			}

			// Verify
			if decoded.UID != tt.auth.UID {
				t.Errorf("UID: got %q, want %q", decoded.UID, tt.auth.UID)
			}
			if decoded.Provider != tt.auth.Provider {
				t.Errorf("Provider: got %q, want %q", decoded.Provider, tt.auth.Provider)
			}
			if decoded.IsTrueAdmin != tt.auth.IsTrueAdmin {
				t.Errorf("IsTrueAdmin: got %v, want %v", decoded.IsTrueAdmin, tt.auth.IsTrueAdmin)
			}

			// Check claims
			for k, v := range tt.auth.Claims {
				if decoded.Claims[k] != v {
					t.Errorf("Claims[%s]: got %v, want %v", k, decoded.Claims[k], v)
				}
			}
		})
	}
}

func TestDecodeAuthPayloadEmpty(t *testing.T) {
	result, err := DecodeAuthPayload(nil)
	if err != nil {
		t.Errorf("DecodeAuthPayload(nil) should not error: %v", err)
	}
	if result != nil {
		t.Errorf("DecodeAuthPayload(nil) should return nil, got %v", result)
	}

	result, err = DecodeAuthPayload([]byte{})
	if err != nil {
		t.Errorf("DecodeAuthPayload([]) should not error: %v", err)
	}
	if result != nil {
		t.Errorf("DecodeAuthPayload([]) should return nil, got %v", result)
	}
}

func TestConnectPayloadWithAuth(t *testing.T) {
	auth := &AuthPayload{
		UID:      "user-456",
		Provider: "password",
		Claims:   map[string]any{"verified": true},
	}
	authBytes, _ := EncodeAuthPayload(auth)

	payload := &ConnectPayload{
		Protocol:   ProtocolWebSocket,
		ProjectID:  "my-project",
		DatabaseID: "my-db",
		Metadata:   []byte(`{"ip":"1.2.3.4"}`),
		Auth:       authBytes,
	}

	// Encode
	encoded, err := EncodeConnectPayload(payload)
	if err != nil {
		t.Fatalf("EncodeConnectPayload failed: %v", err)
	}

	// Decode
	decoded, err := DecodeConnectPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeConnectPayload failed: %v", err)
	}

	// Verify basic fields
	if decoded.Protocol != payload.Protocol {
		t.Errorf("Protocol mismatch")
	}
	if decoded.ProjectID != payload.ProjectID {
		t.Errorf("ProjectID mismatch")
	}
	if decoded.DatabaseID != payload.DatabaseID {
		t.Errorf("DatabaseID mismatch")
	}
	if string(decoded.Metadata) != string(payload.Metadata) {
		t.Errorf("Metadata mismatch")
	}

	// Verify auth
	if !bytes.Equal(decoded.Auth, payload.Auth) {
		t.Errorf("Auth mismatch: got %q, want %q", decoded.Auth, payload.Auth)
	}

	// Decode auth
	decodedAuth, err := DecodeAuthPayload(decoded.Auth)
	if err != nil {
		t.Fatalf("DecodeAuthPayload failed: %v", err)
	}
	if decodedAuth.UID != auth.UID {
		t.Errorf("Auth UID: got %q, want %q", decodedAuth.UID, auth.UID)
	}
}

func TestConnectPayloadBackwardsCompatibility(t *testing.T) {
	// Old format without auth field (backwards compatibility)
	oldPayload := &ConnectPayload{
		Protocol:   ProtocolWebSocket,
		ProjectID:  "project",
		DatabaseID: "db",
		Metadata:   []byte(`{"old":"format"}`),
		// No Auth field
	}

	// Encode using old format (manually, simulating old encoder)
	projectLen := len(oldPayload.ProjectID)
	databaseLen := len(oldPayload.DatabaseID)
	metadataLen := len(oldPayload.Metadata)

	// Old format: protocol(1) + projLen(1) + proj + dbLen(1) + db + metaLen(2) + meta
	// NO auth field
	oldFormatBuf := make([]byte, 1+1+projectLen+1+databaseLen+2+metadataLen)
	offset := 0
	oldFormatBuf[offset] = oldPayload.Protocol
	offset++
	oldFormatBuf[offset] = byte(projectLen)
	offset++
	copy(oldFormatBuf[offset:], oldPayload.ProjectID)
	offset += projectLen
	oldFormatBuf[offset] = byte(databaseLen)
	offset++
	copy(oldFormatBuf[offset:], oldPayload.DatabaseID)
	offset += databaseLen
	oldFormatBuf[offset] = byte(metadataLen >> 8)
	oldFormatBuf[offset+1] = byte(metadataLen)
	offset += 2
	copy(oldFormatBuf[offset:], oldPayload.Metadata)

	// Decode with new decoder - should work
	decoded, err := DecodeConnectPayload(oldFormatBuf)
	if err != nil {
		t.Fatalf("DecodeConnectPayload should handle old format: %v", err)
	}

	if decoded.ProjectID != oldPayload.ProjectID {
		t.Errorf("ProjectID mismatch")
	}
	if decoded.Auth != nil {
		t.Errorf("Auth should be nil for old format, got %v", decoded.Auth)
	}
}

// =============================================================================
// Coordinator Protocol Message Tests
// =============================================================================

func TestEncodeDecodeHeartbeat(t *testing.T) {
	tests := []struct {
		name    string
		payload *HeartbeatPayload
	}{
		{
			name: "basic",
			payload: &HeartbeatPayload{
				Load:    5000, // 50%
				Clients: 100,
				MemMB:   1024,
			},
		},
		{
			name: "zero values",
			payload: &HeartbeatPayload{
				Load:    0,
				Clients: 0,
				MemMB:   0,
			},
		},
		{
			name: "max values",
			payload: &HeartbeatPayload{
				Load:    10000, // 100%
				Clients: 0xFFFFFFFF,
				MemMB:   0xFFFFFFFF,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeHeartbeat(tc.payload)
			decoded, err := DecodeHeartbeat(encoded)
			if err != nil {
				t.Fatalf("DecodeHeartbeat failed: %v", err)
			}

			if decoded.Load != tc.payload.Load {
				t.Errorf("Load: got %d, want %d", decoded.Load, tc.payload.Load)
			}
			if decoded.Clients != tc.payload.Clients {
				t.Errorf("Clients: got %d, want %d", decoded.Clients, tc.payload.Clients)
			}
			if decoded.MemMB != tc.payload.MemMB {
				t.Errorf("MemMB: got %d, want %d", decoded.MemMB, tc.payload.MemMB)
			}
		})
	}
}

func TestDecodeHeartbeatErrors(t *testing.T) {
	// Too short
	_, err := DecodeHeartbeat([]byte{0, 1, 2})
	if err != ErrInvalidMessage {
		t.Errorf("Expected ErrInvalidMessage for short payload, got %v", err)
	}
}

func TestEncodeDecodeHeartbeatAck(t *testing.T) {
	serverTime := uint64(1705000000000) // Some Unix milliseconds

	encoded := EncodeHeartbeatAck(serverTime)
	decoded, err := DecodeHeartbeatAck(encoded)
	if err != nil {
		t.Fatalf("DecodeHeartbeatAck failed: %v", err)
	}

	if decoded.ServerTime != serverTime {
		t.Errorf("ServerTime: got %d, want %d", decoded.ServerTime, serverTime)
	}
}

func TestDecodeHeartbeatAckErrors(t *testing.T) {
	// Too short
	_, err := DecodeHeartbeatAck([]byte{0, 1, 2, 3})
	if err != ErrInvalidMessage {
		t.Errorf("Expected ErrInvalidMessage for short payload, got %v", err)
	}
}

func TestEncodeDecodeDatabaseLoaded(t *testing.T) {
	tests := []struct {
		name       string
		projectID  string
		databaseID string
	}{
		{"basic", "my-project", "my-database"},
		{"short ids", "p", "d"},
		{"longer ids", "project-with-longer-name", "database-with-much-longer-identifier"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeDatabaseLoaded(tc.projectID, tc.databaseID)
			decoded, err := DecodeDatabaseLoaded(encoded)
			if err != nil {
				t.Fatalf("DecodeDatabaseLoaded failed: %v", err)
			}

			if decoded.ProjectID != tc.projectID {
				t.Errorf("ProjectID: got %q, want %q", decoded.ProjectID, tc.projectID)
			}
			if decoded.DatabaseID != tc.databaseID {
				t.Errorf("DatabaseID: got %q, want %q", decoded.DatabaseID, tc.databaseID)
			}
		})
	}
}

func TestDecodeDatabaseLoadedErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{5}},
		{"project overflow", []byte{10, 'a', 'b'}}, // claims 10 bytes but only 2
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeDatabaseLoaded(tc.data)
			if err != ErrInvalidMessage {
				t.Errorf("Expected ErrInvalidMessage, got %v", err)
			}
		})
	}
}

func TestEncodeDecodeDatabaseUnloaded(t *testing.T) {
	tests := []struct {
		name       string
		projectID  string
		databaseID string
		reason     byte
		ephemeral  bool
	}{
		{"idle persistent", "project", "db", UnloadReasonIdle, false},
		{"idle ephemeral", "project", "db", UnloadReasonIdle, true},
		{"memory pressure", "project", "db", UnloadReasonMemoryPressure, false},
		{"evicted", "project", "db", UnloadReasonEvicted, true},
		{"shutdown", "project", "db", UnloadReasonShutdown, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeDatabaseUnloaded(tc.projectID, tc.databaseID, tc.reason, tc.ephemeral)
			decoded, err := DecodeDatabaseUnloaded(encoded)
			if err != nil {
				t.Fatalf("DecodeDatabaseUnloaded failed: %v", err)
			}

			if decoded.ProjectID != tc.projectID {
				t.Errorf("ProjectID: got %q, want %q", decoded.ProjectID, tc.projectID)
			}
			if decoded.DatabaseID != tc.databaseID {
				t.Errorf("DatabaseID: got %q, want %q", decoded.DatabaseID, tc.databaseID)
			}
			if decoded.Reason != tc.reason {
				t.Errorf("Reason: got %d, want %d", decoded.Reason, tc.reason)
			}
			if decoded.Ephemeral != tc.ephemeral {
				t.Errorf("Ephemeral: got %v, want %v", decoded.Ephemeral, tc.ephemeral)
			}
		})
	}
}

func TestDecodeDatabaseUnloadedErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{1, 'a'}},
		{"missing reason and ephemeral", []byte{1, 'a', 1, 'b'}}, // has project and db but no reason/ephemeral
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeDatabaseUnloaded(tc.data)
			if err != ErrInvalidMessage {
				t.Errorf("Expected ErrInvalidMessage, got %v", err)
			}
		})
	}
}

func TestEncodeDecodeConfigRequest(t *testing.T) {
	projectID := "test-project-id"

	encoded := EncodeConfigRequest(projectID)
	decoded, err := DecodeConfigRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeConfigRequest failed: %v", err)
	}

	if decoded.ProjectID != projectID {
		t.Errorf("ProjectID: got %q, want %q", decoded.ProjectID, projectID)
	}
}

func TestDecodeConfigRequestErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"length overflow", []byte{10, 'a', 'b'}}, // claims 10 bytes but only 2
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeConfigRequest(tc.data)
			if err != ErrInvalidMessage {
				t.Errorf("Expected ErrInvalidMessage, got %v", err)
			}
		})
	}
}

func TestEncodeDecodeConfigPush(t *testing.T) {
	projectID := "my-project"
	config := &ProjectConfig{
		Rules:             `{"path": { "read": true }}`,
		SecretKey:         "secret123",
		AdminSecretKey:    "admin456",
		FirebaseProjectID: "firebase-proj",
		Settings: map[string]any{
			"max_clients": 1000,
			"timeout":     30.5,
		},
	}

	encoded, err := EncodeConfigPush(projectID, config)
	if err != nil {
		t.Fatalf("EncodeConfigPush failed: %v", err)
	}

	decoded, err := DecodeConfigPush(encoded)
	if err != nil {
		t.Fatalf("DecodeConfigPush failed: %v", err)
	}

	if decoded.ProjectID != projectID {
		t.Errorf("ProjectID: got %q, want %q", decoded.ProjectID, projectID)
	}
	if decoded.Config.Rules != config.Rules {
		t.Errorf("Rules mismatch")
	}
	if decoded.Config.SecretKey != config.SecretKey {
		t.Errorf("SecretKey mismatch")
	}
	if decoded.Config.AdminSecretKey != config.AdminSecretKey {
		t.Errorf("AdminSecretKey mismatch")
	}
	if decoded.Config.FirebaseProjectID != config.FirebaseProjectID {
		t.Errorf("FirebaseProjectID mismatch")
	}
}

func TestDecodeConfigPushErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{1, 'a', 0, 0}},
		{"config length overflow", []byte{1, 'a', 0, 0, 0, 100}}, // claims 100 bytes of JSON
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeConfigPush(tc.data)
			if err == nil {
				t.Error("Expected error for invalid payload")
			}
		})
	}
}

func TestEncodeDecodeEvictDatabase(t *testing.T) {
	projectID := "project"
	databaseID := "database"

	for _, purge := range []bool{false, true} {
		encoded := EncodeEvictDatabase(projectID, databaseID, purge)
		decoded, err := DecodeEvictDatabase(encoded)
		if err != nil {
			t.Fatalf("DecodeEvictDatabase(purge=%v) failed: %v", purge, err)
		}

		if decoded.ProjectID != projectID {
			t.Errorf("ProjectID: got %q, want %q", decoded.ProjectID, projectID)
		}
		if decoded.DatabaseID != databaseID {
			t.Errorf("DatabaseID: got %q, want %q", decoded.DatabaseID, databaseID)
		}
		if decoded.Purge != purge {
			t.Errorf("Purge: got %v, want %v", decoded.Purge, purge)
		}
	}

	// Legacy payload without trailing flags byte should decode as Purge=false.
	legacy := EncodeDatabaseLoaded(projectID, databaseID)
	decoded, err := DecodeEvictDatabase(legacy)
	if err != nil {
		t.Fatalf("DecodeEvictDatabase(legacy) failed: %v", err)
	}
	if decoded.Purge {
		t.Error("legacy payload should decode with Purge=false")
	}
}

func TestEncodeDecodeShutdown(t *testing.T) {
	gracePeriod := uint32(30)

	encoded := EncodeShutdown(gracePeriod)
	decoded, err := DecodeShutdown(encoded)
	if err != nil {
		t.Fatalf("DecodeShutdown failed: %v", err)
	}

	if decoded.GracePeriodSec != gracePeriod {
		t.Errorf("GracePeriodSec: got %d, want %d", decoded.GracePeriodSec, gracePeriod)
	}
}

func TestDecodeShutdownErrors(t *testing.T) {
	_, err := DecodeShutdown([]byte{0, 1, 2})
	if err != ErrInvalidMessage {
		t.Errorf("Expected ErrInvalidMessage for short payload, got %v", err)
	}
}

func TestWriteReadControlMessage(t *testing.T) {
	tests := []struct {
		name    string
		msgType byte
		payload []byte
	}{
		{"heartbeat ack", MsgTypeHeartbeatAck, EncodeHeartbeatAck(1705000000000)},
		{"config push", MsgTypeConfigPush, func() []byte {
			data, _ := EncodeConfigPush("proj", &ProjectConfig{Rules: "{}"})
			return data
		}()},
		{"evict database", MsgTypeEvictDatabase, EncodeEvictDatabase("proj", "db", false)},
		{"evict database purge", MsgTypeEvictDatabase, EncodeEvictDatabase("proj", "db", true)},
		{"shutdown", MsgTypeShutdown, EncodeShutdown(30)},
		{"empty payload", MsgTypeHeartbeatAck, []byte{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			err := WriteControlMessage(&buf, tc.msgType, tc.payload)
			if err != nil {
				t.Fatalf("WriteControlMessage failed: %v", err)
			}

			msg, err := ReadControlMessage(&buf)
			if err != nil {
				t.Fatalf("ReadControlMessage failed: %v", err)
			}

			if msg.Type != tc.msgType {
				t.Errorf("Type: got %d, want %d", msg.Type, tc.msgType)
			}
			if !bytes.Equal(msg.Payload, tc.payload) {
				t.Errorf("Payload mismatch")
			}
		})
	}
}

func TestCoreForDatabase(t *testing.T) {
	// Test consistency
	db1 := "my-database"
	nrCores := 8

	core1 := CoreForDatabase(db1, nrCores)
	core2 := CoreForDatabase(db1, nrCores)

	if core1 != core2 {
		t.Errorf("CoreForDatabase should be deterministic: got %d and %d", core1, core2)
	}

	// Verify range
	if core1 < 0 || core1 >= nrCores {
		t.Errorf("Core %d out of range [0, %d)", core1, nrCores)
	}

	// Test different database gives different core (likely)
	db2 := "other-database"
	core3 := CoreForDatabase(db2, nrCores)
	// Not checking equality - just ensuring it doesn't panic

	_ = core3 // silence unused warning
}

// =============================================================================
// Compressed Multi Message Tests
// =============================================================================

func TestDecodeCompressedMultiPayload(t *testing.T) {
	tests := []struct {
		name     string
		messages []CompressedMultiMessage
	}{
		{
			name: "single message",
			messages: []CompressedMultiMessage{
				{ClientID: 1, Data: []byte("hello")},
			},
		},
		{
			name: "multiple messages",
			messages: []CompressedMultiMessage{
				{ClientID: 1, Data: []byte("message one")},
				{ClientID: 2, Data: []byte("message two")},
				{ClientID: 3, Data: []byte("message three")},
			},
		},
		{
			name: "many messages",
			messages: func() []CompressedMultiMessage {
				msgs := make([]CompressedMultiMessage, 100)
				for i := range msgs {
					msgs[i] = CompressedMultiMessage{
						ClientID: uint32(i + 1),
						Data:     []byte("test payload data"),
					}
				}
				return msgs
			}(),
		},
		{
			name: "empty payload messages",
			messages: []CompressedMultiMessage{
				{ClientID: 1, Data: []byte{}},
				{ClientID: 2, Data: []byte{}},
			},
		},
		{
			name: "large payloads",
			messages: []CompressedMultiMessage{
				{ClientID: 1, Data: bytes.Repeat([]byte("x"), 10000)},
				{ClientID: 2, Data: bytes.Repeat([]byte("y"), 5000)},
			},
		},
		{
			name: "max client IDs",
			messages: []CompressedMultiMessage{
				{ClientID: 0, Data: []byte("zero")},
				{ClientID: 0xFFFFFFFF, Data: []byte("max")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the payload manually
			encoded := encodeCompressedMultiPayload(tt.messages)

			// Decode
			decoded, err := DecodeCompressedMultiPayload(encoded)
			if err != nil {
				t.Fatalf("DecodeCompressedMultiPayload failed: %v", err)
			}

			// Verify
			if len(decoded) != len(tt.messages) {
				t.Fatalf("Message count: got %d, want %d", len(decoded), len(tt.messages))
			}

			for i, want := range tt.messages {
				got := decoded[i]
				if got.ClientID != want.ClientID {
					t.Errorf("Message %d ClientID: got %d, want %d", i, got.ClientID, want.ClientID)
				}
				if !bytes.Equal(got.Data, want.Data) {
					t.Errorf("Message %d Data: got %v, want %v", i, got.Data, want.Data)
				}
			}
		})
	}
}

func TestDecodeCompressedMultiPayloadErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty",
			data: []byte{},
		},
		{
			name: "too short for count",
			data: []byte{0, 0, 0},
		},
		{
			name: "count but no messages",
			data: []byte{0, 0, 0, 1}, // claims 1 message but no data
		},
		{
			name: "truncated client ID",
			data: []byte{0, 0, 0, 1, 0, 0}, // claims 1 message, only 2 bytes of client ID
		},
		{
			name: "truncated message length",
			data: []byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0}, // client ID complete, length truncated
		},
		{
			name: "message length overflow",
			data: []byte{
				0, 0, 0, 1, // count = 1
				0, 0, 0, 1, // clientID = 1
				0, 0, 1, 0, // msgLen = 256 (but no data follows)
			},
		},
		{
			name: "count too large",
			data: []byte{0x10, 0x00, 0x00, 0x00}, // count = 268435456 (> 1000000 limit)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeCompressedMultiPayload(tt.data)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestDecodeCompressedMultiPayloadEmpty(t *testing.T) {
	// Zero messages is valid
	data := []byte{0, 0, 0, 0} // count = 0
	messages, err := DecodeCompressedMultiPayload(data)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(messages))
	}
}

// Helper function to encode compressed multi payload for testing
func encodeCompressedMultiPayload(messages []CompressedMultiMessage) []byte {
	// Calculate total size
	size := 4 // count
	for _, m := range messages {
		size += 4 + 4 + len(m.Data) // clientID + msgLen + data
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(messages)))

	offset := 4
	for _, m := range messages {
		binary.BigEndian.PutUint32(buf[offset:], m.ClientID)
		binary.BigEndian.PutUint32(buf[offset+4:], uint32(len(m.Data)))
		copy(buf[offset+8:], m.Data)
		offset += 8 + len(m.Data)
	}

	return buf
}

// =============================================================================
// Broadcast Message Tests
// =============================================================================

func TestDecodeBroadcastPayload(t *testing.T) {
	tests := []struct {
		name    string
		clients []BroadcastClient
		message []byte
	}{
		{
			name: "single client no tag",
			clients: []BroadcastClient{
				{ID: 1, Tag: 0},
			},
			message: []byte(`{"type":"update"}`),
		},
		{
			name: "single client with tag",
			clients: []BroadcastClient{
				{ID: 1, Tag: 42},
			},
			message: []byte(`{"type":"update"}`),
		},
		{
			name: "multiple clients mixed tags",
			clients: []BroadcastClient{
				{ID: 1, Tag: 0},
				{ID: 2, Tag: 100},
				{ID: 3, Tag: -50}, // negative tag
			},
			message: []byte(`{"type":"update","data":{"value":123}}`),
		},
		{
			name: "many clients",
			clients: func() []BroadcastClient {
				clients := make([]BroadcastClient, 1000)
				for i := range clients {
					clients[i] = BroadcastClient{ID: uint32(i + 1), Tag: int32(i)}
				}
				return clients
			}(),
			message: []byte("broadcast payload"),
		},
		{
			name:    "empty message",
			clients: []BroadcastClient{{ID: 1, Tag: 0}},
			message: []byte{},
		},
		{
			name: "large message",
			clients: []BroadcastClient{
				{ID: 1, Tag: 0},
				{ID: 2, Tag: 0},
			},
			message: bytes.Repeat([]byte("x"), 100000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the payload manually
			encoded := encodeBroadcastPayload(tt.clients, tt.message)

			// Decode
			decoded, err := DecodeBroadcastPayload(encoded)
			if err != nil {
				t.Fatalf("DecodeBroadcastPayload failed: %v", err)
			}

			// Verify clients
			if len(decoded.Clients) != len(tt.clients) {
				t.Fatalf("Client count: got %d, want %d", len(decoded.Clients), len(tt.clients))
			}
			for i, want := range tt.clients {
				got := decoded.Clients[i]
				if got.ID != want.ID {
					t.Errorf("Client %d ID: got %d, want %d", i, got.ID, want.ID)
				}
				if got.Tag != want.Tag {
					t.Errorf("Client %d Tag: got %d, want %d", i, got.Tag, want.Tag)
				}
			}

			// Verify message
			if !bytes.Equal(decoded.Message, tt.message) {
				t.Errorf("Message mismatch: got %d bytes, want %d bytes", len(decoded.Message), len(tt.message))
			}
		})
	}
}

func TestDecodeBroadcastPayloadErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty",
			data: []byte{},
		},
		{
			name: "too short for count",
			data: []byte{0, 0, 0},
		},
		{
			name: "count but no clients",
			data: []byte{0, 0, 0, 1}, // claims 1 client but no data
		},
		{
			name: "truncated client list",
			data: []byte{
				0, 0, 0, 2, // count = 2
				0, 0, 0, 1, 0, 0, 0, 0, // client 1 complete
				0, 0, 0, 2, // client 2 ID only, no tag
			},
		},
		{
			name: "missing message length",
			data: []byte{
				0, 0, 0, 1, // count = 1
				0, 0, 0, 1, 0, 0, 0, 0, // client 1
				// no msgLen
			},
		},
		{
			name: "message length overflow",
			data: []byte{
				0, 0, 0, 1, // count = 1
				0, 0, 0, 1, 0, 0, 0, 0, // client 1
				0, 0, 1, 0, // msgLen = 256 (but no data)
			},
		},
		{
			name: "client count too large",
			data: []byte{0x00, 0x10, 0x00, 0x00}, // count = 1048576 (> 262144 limit)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeBroadcastPayload(tt.data)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestDecodeBroadcastPayloadZeroClients(t *testing.T) {
	// Zero clients with a message is valid (edge case)
	data := encodeBroadcastPayload([]BroadcastClient{}, []byte("ignored message"))
	result, err := DecodeBroadcastPayload(data)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(result.Clients) != 0 {
		t.Errorf("Expected 0 clients, got %d", len(result.Clients))
	}
}

// Helper function to encode broadcast payload for testing
func encodeBroadcastPayload(clients []BroadcastClient, message []byte) []byte {
	// Calculate size: count(4) + clients(8 each) + msgLen(4) + message
	size := 4 + len(clients)*8 + 4 + len(message)
	buf := make([]byte, size)

	binary.BigEndian.PutUint32(buf[0:4], uint32(len(clients)))

	offset := 4
	for _, c := range clients {
		binary.BigEndian.PutUint32(buf[offset:], c.ID)
		binary.BigEndian.PutUint32(buf[offset+4:], uint32(c.Tag))
		offset += 8
	}

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(message)))
	offset += 4
	copy(buf[offset:], message)

	return buf
}

// TestMessageTypeUniqueness verifies that message types don't conflict
func TestMessageTypeUniqueness(t *testing.T) {
	// Ensure message types don't conflict
	allProxyToBackend := []byte{
		MsgTypeConnect,
		MsgTypeData,
		MsgTypeDisconnect,
		MsgTypeHello,
		MsgTypeAuthChanged,
		MsgTypeHeartbeatAck,
		MsgTypeConfigPush,
		MsgTypeEvictDatabase,
		MsgTypeShutdown,
		MsgTypeHelloAuth,
	}

	seen := make(map[byte]bool)
	for _, mt := range allProxyToBackend {
		if seen[mt] {
			t.Errorf("Duplicate proxy->backend message type: 0x%02x", mt)
		}
		seen[mt] = true
	}

	allBackendToProxy := []byte{
		MsgTypeSendData,
		MsgTypeClose,
		MsgTypeHelloAck,
		MsgTypeHeartbeat,
		MsgTypeDatabaseLoaded,
		MsgTypeDatabaseUnloaded,
		MsgTypeConfigRequest,
		MsgTypeCompressedMulti,
		MsgTypeBroadcast,
	}

	seen = make(map[byte]bool)
	for _, mt := range allBackendToProxy {
		if seen[mt] {
			t.Errorf("Duplicate backend->proxy message type: 0x%02x", mt)
		}
		seen[mt] = true
	}
}
