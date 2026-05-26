package backend

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// mockClient implements the Client interface for testing
type mockClient struct {
	mu         sync.Mutex
	messages   []mockDelivery
	closed     bool
	deliverErr bool // if true, Deliver returns false
}

type mockDelivery struct {
	payload  []byte
	reliable bool
}

func newMockClient() *mockClient {
	return &mockClient{
		messages: make([]mockDelivery, 0),
	}
}

func (c *mockClient) Deliver(payload []byte, reliable bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deliverErr || c.closed {
		return false
	}
	// Copy payload since it may be reused
	cp := make([]byte, len(payload))
	copy(cp, payload)
	c.messages = append(c.messages, mockDelivery{payload: cp, reliable: reliable})
	return true
}

func (c *mockClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

func (c *mockClient) getMessages() []mockDelivery {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]mockDelivery, len(c.messages))
	copy(result, c.messages)
	return result
}

func (c *mockClient) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// mockClientRegistry implements ClientRegistry for testing
type mockClientRegistry struct {
	mu      sync.RWMutex
	clients map[uint32]*mockClient
}

func newMockClientRegistry() *mockClientRegistry {
	return &mockClientRegistry{
		clients: make(map[uint32]*mockClient),
	}
}

func (r *mockClientRegistry) GetClient(clientID uint32) Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if c, ok := r.clients[clientID]; ok {
		return c
	}
	return nil
}

func (r *mockClientRegistry) addClient(clientID uint32, client *mockClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[clientID] = client
}

// Helper to create a COMPRESSED_MULTI wire message
func createCompressedMultiMessage(messages []CompressedMultiMessage, reliable bool) []byte {
	// Encode the inner payload
	innerSize := 4 // count
	for _, m := range messages {
		innerSize += 8 + len(m.Data)
	}

	inner := make([]byte, innerSize)
	binary.BigEndian.PutUint32(inner[0:4], uint32(len(messages)))
	offset := 4
	for _, m := range messages {
		binary.BigEndian.PutUint32(inner[offset:], m.ClientID)
		binary.BigEndian.PutUint32(inner[offset+4:], uint32(len(m.Data)))
		copy(inner[offset+8:], m.Data)
		offset += 8 + len(m.Data)
	}

	// Compress with ZSTD
	encoder, _ := zstd.NewWriter(nil)
	compressed := encoder.EncodeAll(inner, nil)

	// Build wire message: [Type:1][Flags:1][CompressedPayload:var]
	flags := byte(FlagCompressed)
	if reliable {
		flags |= FlagReliable
	}

	wireMsg := make([]byte, 2+len(compressed))
	wireMsg[0] = MsgTypeCompressedMulti
	wireMsg[1] = flags
	copy(wireMsg[2:], compressed)

	return wireMsg
}

// mockBackendServer creates a server that sends COMPRESSED_MULTI messages
func mockBackendServer(t *testing.T, nrCores uint8, messagesToSend [][]byte) (net.Listener, func(), *atomic.Int32) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	done := make(chan struct{})
	connectionCount := &atomic.Int32{}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			// Read HELLO message
			lenBuf := make([]byte, 4)
			if _, err := conn.Read(lenBuf); err != nil {
				conn.Close()
				continue
			}

			msgLen := binary.BigEndian.Uint32(lenBuf)
			msg := make([]byte, msgLen)
			if _, err := conn.Read(msg); err != nil {
				conn.Close()
				continue
			}

			// Verify it's a HELLO
			if msg[0] != MsgTypeHello {
				conn.Close()
				continue
			}

			// Send HELLO_ACK
			coreID := uint8(connectionCount.Add(1)-1) % nrCores
			resp := make([]byte, 13)
			binary.BigEndian.PutUint32(resp[0:4], 9)
			resp[4] = MsgTypeHelloAck
			resp[5] = coreID
			resp[6] = nrCores
			binary.BigEndian.PutUint16(resp[7:9], 1)
			conn.Write(resp)

			// Send the test messages
			go func(c net.Conn) {
				defer c.Close()

				// Small delay to let readLoop start
				time.Sleep(50 * time.Millisecond)

				for _, wireMsg := range messagesToSend {
					// Send as wire protocol frame: [Length:4][Payload:var]
					frame := make([]byte, 4+len(wireMsg))
					binary.BigEndian.PutUint32(frame[0:4], uint32(len(wireMsg)))
					copy(frame[4:], wireMsg)
					c.Write(frame)
				}

				// Wait for done signal
				<-done
			}(conn)
		}
	}()

	cleanup := func() {
		close(done)
		listener.Close()
	}

	return listener, cleanup, connectionCount
}

func TestCompressedMultiDispatch(t *testing.T) {
	// Create mock clients
	registry := newMockClientRegistry()
	client1 := newMockClient()
	client2 := newMockClient()
	client3 := newMockClient()
	registry.addClient(1, client1)
	registry.addClient(2, client2)
	registry.addClient(3, client3)

	// Create COMPRESSED_MULTI message with data for all 3 clients
	compressedMsg := createCompressedMultiMessage([]CompressedMultiMessage{
		{ClientID: 1, Data: []byte("hello client 1")},
		{ClientID: 2, Data: []byte("hello client 2")},
		{ClientID: 3, Data: []byte("hello client 3")},
	}, true) // reliable

	// Create mock server that will send the compressed message
	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{compressedMsg})
	defer cleanup()

	// Create pool and add backend
	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	// Wait for messages to be delivered
	time.Sleep(200 * time.Millisecond)

	// Verify each client received their message
	msgs1 := client1.getMessages()
	if len(msgs1) != 1 {
		t.Errorf("Client 1: expected 1 message, got %d", len(msgs1))
	} else {
		if !bytes.Equal(msgs1[0].payload, []byte("hello client 1")) {
			t.Errorf("Client 1: wrong payload: %q", msgs1[0].payload)
		}
		if !msgs1[0].reliable {
			t.Error("Client 1: expected reliable=true")
		}
	}

	msgs2 := client2.getMessages()
	if len(msgs2) != 1 {
		t.Errorf("Client 2: expected 1 message, got %d", len(msgs2))
	} else if !bytes.Equal(msgs2[0].payload, []byte("hello client 2")) {
		t.Errorf("Client 2: wrong payload: %q", msgs2[0].payload)
	}

	msgs3 := client3.getMessages()
	if len(msgs3) != 1 {
		t.Errorf("Client 3: expected 1 message, got %d", len(msgs3))
	} else if !bytes.Equal(msgs3[0].payload, []byte("hello client 3")) {
		t.Errorf("Client 3: wrong payload: %q", msgs3[0].payload)
	}
}

func TestCompressedMultiUnreliable(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	// Create COMPRESSED_MULTI with unreliable flag
	compressedMsg := createCompressedMultiMessage([]CompressedMultiMessage{
		{ClientID: 1, Data: []byte("unreliable data")},
	}, false) // unreliable

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{compressedMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if msgs[0].reliable {
		t.Error("Expected reliable=false for unreliable message")
	}
}

func TestCompressedMultiMissingClient(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)
	// Note: client 2 is NOT registered

	// Create COMPRESSED_MULTI with messages for both existing and non-existing clients
	compressedMsg := createCompressedMultiMessage([]CompressedMultiMessage{
		{ClientID: 1, Data: []byte("for existing client")},
		{ClientID: 2, Data: []byte("for missing client")}, // This client doesn't exist
	}, true)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{compressedMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client 1 should still receive their message
	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message for client 1, got %d", len(msgs))
	}
	// No panic or error should occur for missing client 2
}

func TestCompressedMultiClientFull(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	client1.deliverErr = true // Simulate full outbox
	registry.addClient(1, client1)

	compressedMsg := createCompressedMultiMessage([]CompressedMultiMessage{
		{ClientID: 1, Data: []byte("data")},
	}, true)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{compressedMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client should be closed due to full outbox
	if !client1.isClosed() {
		t.Error("Expected client to be closed when outbox is full")
	}
}

func TestCompressedMultiLargeBatch(t *testing.T) {
	registry := newMockClientRegistry()
	clients := make([]*mockClient, 100)
	for i := range clients {
		clients[i] = newMockClient()
		registry.addClient(uint32(i+1), clients[i])
	}

	// Create a large batch of 100 messages
	msgs := make([]CompressedMultiMessage, 100)
	for i := range msgs {
		msgs[i] = CompressedMultiMessage{
			ClientID: uint32(i + 1),
			Data:     []byte("shared view update payload"),
		}
	}

	compressedMsg := createCompressedMultiMessage(msgs, true)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{compressedMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Verify all 100 clients received their message
	for i, client := range clients {
		messages := client.getMessages()
		if len(messages) != 1 {
			t.Errorf("Client %d: expected 1 message, got %d", i+1, len(messages))
		}
	}
}

func TestInlineDispatchSendDataCompressed(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	// Create a compressed SEND_DATA message
	// Format: [Type:1][ClientID:4][Flags:1][Data:var]
	originalPayload := []byte("this is the original uncompressed data that will be sent to the client")

	// Compress the payload
	encoder, _ := zstd.NewWriter(nil)
	compressedPayload := encoder.EncodeAll(originalPayload, nil)

	wireMsg := make([]byte, 1+4+1+len(compressedPayload))
	wireMsg[0] = MsgTypeSendData
	binary.BigEndian.PutUint32(wireMsg[1:5], 1) // clientID = 1
	wireMsg[5] = FlagReliable | FlagCompressed  // Both flags set
	copy(wireMsg[6:], compressedPayload)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{wireMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client should receive the DECOMPRESSED payload
	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if !bytes.Equal(msgs[0].payload, originalPayload) {
		t.Errorf("Wrong payload: got %q, want %q", msgs[0].payload, originalPayload)
	}
	if !msgs[0].reliable {
		t.Error("Expected reliable=true")
	}
}

func TestInlineDispatchSendDataCompressedUnreliable(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	originalPayload := []byte("unreliable compressed data")

	encoder, _ := zstd.NewWriter(nil)
	compressedPayload := encoder.EncodeAll(originalPayload, nil)

	wireMsg := make([]byte, 1+4+1+len(compressedPayload))
	wireMsg[0] = MsgTypeSendData
	binary.BigEndian.PutUint32(wireMsg[1:5], 1) // clientID = 1
	wireMsg[5] = FlagCompressed                 // Compressed but unreliable (no FlagReliable)
	copy(wireMsg[6:], compressedPayload)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{wireMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if !bytes.Equal(msgs[0].payload, originalPayload) {
		t.Errorf("Wrong payload: got %q, want %q", msgs[0].payload, originalPayload)
	}
	if msgs[0].reliable {
		t.Error("Expected reliable=false for unreliable message")
	}
}

func TestInlineDispatchSendData(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	// Create a regular SEND_DATA message (not COMPRESSED_MULTI)
	// Format: [Type:1][ClientID:4][Flags:1][Data:var]
	payload := []byte("regular data message")
	wireMsg := make([]byte, 1+4+1+len(payload))
	wireMsg[0] = MsgTypeSendData
	binary.BigEndian.PutUint32(wireMsg[1:5], 1) // clientID = 1
	wireMsg[5] = FlagReliable
	copy(wireMsg[6:], payload)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{wireMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if !bytes.Equal(msgs[0].payload, payload) {
		t.Errorf("Wrong payload: got %q, want %q", msgs[0].payload, payload)
	}
	if !msgs[0].reliable {
		t.Error("Expected reliable=true")
	}
}

func TestInlineDispatchClose(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	// Create a CLOSE message
	// Format: [Type:1][ClientID:4]
	wireMsg := make([]byte, 5)
	wireMsg[0] = MsgTypeClose
	binary.BigEndian.PutUint32(wireMsg[1:5], 1) // clientID = 1

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{wireMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !client1.isClosed() {
		t.Error("Expected client to be closed")
	}
}

// =============================================================================
// Tag Insertion Unit Tests
// =============================================================================

func TestInsertTagFirebase(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		tag      int32
		expected string
	}{
		{
			name:     "basic message",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":1}}}`,
			tag:      7,
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":1,"t":7}}}`,
		},
		{
			name:     "negative tag",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":1}}}`,
			tag:      -3,
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":1,"t":-3}}}`,
		},
		{
			name:     "null value",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":null}}}`,
			tag:      42,
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":null,"t":42}}}`,
		},
		{
			name:     "empty object value",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":{}}}}`,
			tag:      1,
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":{},"t":1}}}`,
		},
		{
			name:     "nested object value",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/users/alice","d":{"name":"Alice"}}}}`,
			tag:      42,
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/users/alice","d":{"name":"Alice"},"t":42}}}`,
		},
		{
			name:     "array value",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":[1,2,3]}}}`,
			tag:      99,
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":[1,2,3],"t":99}}}`,
		},
		{
			name:     "large tag",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":1}}}`,
			tag:      2147483647, // max int32
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":1,"t":2147483647}}}`,
		},
		{
			name:     "min negative tag",
			input:    `{"t":"d","d":{"a":"d","b":{"p":"/","d":1}}}`,
			tag:      -2147483648, // min int32
			expected: `{"t":"d","d":{"a":"d","b":{"p":"/","d":1,"t":-2147483648}}}`,
		},
		{
			name:     "invalid format - doesn't end with }}}",
			input:    `{"simple":"json"}`,
			tag:      1,
			expected: `{"simple":"json"}`, // unchanged
		},
		{
			name:     "too short",
			input:    `{}`,
			tag:      1,
			expected: `{}`, // unchanged
		},
		{
			name:     "empty",
			input:    ``,
			tag:      1,
			expected: ``, // unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := insertTagFirebase([]byte(tt.input), tt.tag)
			if string(result) != tt.expected {
				t.Errorf("insertTagFirebase(%q, %d)\ngot:  %q\nwant: %q", tt.input, tt.tag, string(result), tt.expected)
			}
		})
	}
}

func TestInsertTagLark(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		tag      int32
		expected string
	}{
		{
			name:     "basic message",
			input:    `{"o":"d","v":1}`,
			tag:      7,
			expected: `{"tag":7,"o":"d","v":1}`,
		},
		{
			name:     "negative tag",
			input:    `{"o":"d","v":1}`,
			tag:      -3,
			expected: `{"tag":-3,"o":"d","v":1}`,
		},
		{
			name:     "complex message",
			input:    `{"o":"d","p":"/users/alice","v":{"name":"Alice","score":100}}`,
			tag:      42,
			expected: `{"tag":42,"o":"d","p":"/users/alice","v":{"name":"Alice","score":100}}`,
		},
		{
			name:     "large tag",
			input:    `{"o":"d","v":1}`,
			tag:      2147483647, // max int32
			expected: `{"tag":2147483647,"o":"d","v":1}`,
		},
		{
			name:     "min negative tag",
			input:    `{"o":"d","v":1}`,
			tag:      -2147483648, // min int32
			expected: `{"tag":-2147483648,"o":"d","v":1}`,
		},
		{
			name:     "invalid format - doesn't start with {",
			input:    `["array"]`,
			tag:      1,
			expected: `["array"]`, // unchanged
		},
		{
			name:     "empty",
			input:    ``,
			tag:      1,
			expected: ``, // unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := insertTagLark([]byte(tt.input), tt.tag)
			if string(result) != tt.expected {
				t.Errorf("insertTagLark(%q, %d)\ngot:  %q\nwant: %q", tt.input, tt.tag, string(result), tt.expected)
			}
		})
	}
}

// =============================================================================
// BROADCAST Tests
// =============================================================================

// Helper to create a BROADCAST wire message
func createBroadcastMessage(clients []BroadcastClient, message []byte, flags byte) []byte {
	// Encode payload: [ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes:var]
	payloadSize := 4 + len(clients)*8 + 4 + len(message)
	payload := make([]byte, payloadSize)

	binary.BigEndian.PutUint32(payload[0:4], uint32(len(clients)))

	offset := 4
	for _, c := range clients {
		binary.BigEndian.PutUint32(payload[offset:], c.ID)
		binary.BigEndian.PutUint32(payload[offset+4:], uint32(c.Tag))
		offset += 8
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(message)))
	offset += 4
	copy(payload[offset:], message)

	// Build wire message: [Type:1][Flags:1][Payload:var]
	wireMsg := make([]byte, 2+len(payload))
	wireMsg[0] = MsgTypeBroadcast
	wireMsg[1] = flags
	copy(wireMsg[2:], payload)

	return wireMsg
}

func TestBroadcastDispatch(t *testing.T) {
	// Create mock clients
	registry := newMockClientRegistry()
	client1 := newMockClient()
	client2 := newMockClient()
	client3 := newMockClient()
	registry.addClient(1, client1)
	registry.addClient(2, client2)
	registry.addClient(3, client3)

	// Create BROADCAST message - same message to all 3 clients
	broadcastMsg := createBroadcastMessage(
		[]BroadcastClient{
			{ID: 1, Tag: 0},
			{ID: 2, Tag: 0},
			{ID: 3, Tag: 0},
		},
		[]byte(`{"type":"shared_update","data":"hello everyone"}`),
		BroadcastFlagReliable,
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// All clients should receive the same message
	expectedPayload := []byte(`{"type":"shared_update","data":"hello everyone"}`)

	for i, client := range []*mockClient{client1, client2, client3} {
		msgs := client.getMessages()
		if len(msgs) != 1 {
			t.Errorf("Client %d: expected 1 message, got %d", i+1, len(msgs))
			continue
		}
		if !bytes.Equal(msgs[0].payload, expectedPayload) {
			t.Errorf("Client %d: wrong payload: %q", i+1, msgs[0].payload)
		}
		if !msgs[0].reliable {
			t.Errorf("Client %d: expected reliable=true", i+1)
		}
	}
}

func TestBroadcastUnreliable(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	broadcastMsg := createBroadcastMessage(
		[]BroadcastClient{{ID: 1, Tag: 0}},
		[]byte("unreliable broadcast"),
		0, // no flags = unreliable
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if msgs[0].reliable {
		t.Error("Expected reliable=false for unreliable broadcast")
	}
}

func TestBroadcastWithTagsLark(t *testing.T) {
	// Lark format: tag is inserted after opening {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	client2 := newMockClient()
	client3 := newMockClient()
	registry.addClient(1, client1)
	registry.addClient(2, client2)
	registry.addClient(3, client3)

	broadcastMsg := createBroadcastMessage(
		[]BroadcastClient{
			{ID: 1, Tag: 42},   // Positive tag
			{ID: 2, Tag: -100}, // Negative tag
			{ID: 3, Tag: 0},    // No tag (message unchanged)
		},
		[]byte(`{"o":"d","p":"/users/alice","v":{"name":"Alice"}}`),
		BroadcastFlagReliable, // No FIREBASE flag = Lark format
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client 1: tag=42 inserted
	msgs1 := client1.getMessages()
	if len(msgs1) != 1 {
		t.Fatalf("Client 1: expected 1 message, got %d", len(msgs1))
	}
	expected1 := `{"tag":42,"o":"d","p":"/users/alice","v":{"name":"Alice"}}`
	if string(msgs1[0].payload) != expected1 {
		t.Errorf("Client 1: wrong payload\ngot:  %s\nwant: %s", msgs1[0].payload, expected1)
	}

	// Client 2: tag=-100 inserted
	msgs2 := client2.getMessages()
	if len(msgs2) != 1 {
		t.Fatalf("Client 2: expected 1 message, got %d", len(msgs2))
	}
	expected2 := `{"tag":-100,"o":"d","p":"/users/alice","v":{"name":"Alice"}}`
	if string(msgs2[0].payload) != expected2 {
		t.Errorf("Client 2: wrong payload\ngot:  %s\nwant: %s", msgs2[0].payload, expected2)
	}

	// Client 3: tag=0, message unchanged
	msgs3 := client3.getMessages()
	if len(msgs3) != 1 {
		t.Fatalf("Client 3: expected 1 message, got %d", len(msgs3))
	}
	expected3 := `{"o":"d","p":"/users/alice","v":{"name":"Alice"}}`
	if string(msgs3[0].payload) != expected3 {
		t.Errorf("Client 3: wrong payload\ngot:  %s\nwant: %s", msgs3[0].payload, expected3)
	}
}

func TestBroadcastWithTagsFirebase(t *testing.T) {
	// Firebase format: tag is inserted before final }}}
	registry := newMockClientRegistry()
	client1 := newMockClient()
	client2 := newMockClient()
	client3 := newMockClient()
	registry.addClient(1, client1)
	registry.addClient(2, client2)
	registry.addClient(3, client3)

	broadcastMsg := createBroadcastMessage(
		[]BroadcastClient{
			{ID: 1, Tag: 7},  // Positive tag
			{ID: 2, Tag: -3}, // Negative tag
			{ID: 3, Tag: 0},  // No tag (message unchanged)
		},
		[]byte(`{"t":"d","d":{"a":"d","b":{"p":"/","d":{"value":123}}}}`),
		BroadcastFlagReliable|BroadcastFlagFirebase,
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client 1: tag=7 inserted before }}}
	msgs1 := client1.getMessages()
	if len(msgs1) != 1 {
		t.Fatalf("Client 1: expected 1 message, got %d", len(msgs1))
	}
	expected1 := `{"t":"d","d":{"a":"d","b":{"p":"/","d":{"value":123},"t":7}}}`
	if string(msgs1[0].payload) != expected1 {
		t.Errorf("Client 1: wrong payload\ngot:  %s\nwant: %s", msgs1[0].payload, expected1)
	}

	// Client 2: tag=-3 inserted
	msgs2 := client2.getMessages()
	if len(msgs2) != 1 {
		t.Fatalf("Client 2: expected 1 message, got %d", len(msgs2))
	}
	expected2 := `{"t":"d","d":{"a":"d","b":{"p":"/","d":{"value":123},"t":-3}}}`
	if string(msgs2[0].payload) != expected2 {
		t.Errorf("Client 2: wrong payload\ngot:  %s\nwant: %s", msgs2[0].payload, expected2)
	}

	// Client 3: tag=0, message unchanged
	msgs3 := client3.getMessages()
	if len(msgs3) != 1 {
		t.Fatalf("Client 3: expected 1 message, got %d", len(msgs3))
	}
	expected3 := `{"t":"d","d":{"a":"d","b":{"p":"/","d":{"value":123}}}}`
	if string(msgs3[0].payload) != expected3 {
		t.Errorf("Client 3: wrong payload\ngot:  %s\nwant: %s", msgs3[0].payload, expected3)
	}
}

func TestBroadcastMissingClient(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)
	// Client 2 is NOT registered

	broadcastMsg := createBroadcastMessage(
		[]BroadcastClient{
			{ID: 1, Tag: 0},
			{ID: 2, Tag: 0}, // Missing client
		},
		[]byte("broadcast data"),
		BroadcastFlagReliable,
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client 1 should still receive the message
	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message for client 1, got %d", len(msgs))
	}
	// No panic for missing client 2
}

func TestBroadcastLargeFanout(t *testing.T) {
	registry := newMockClientRegistry()
	clients := make([]*mockClient, 500)
	broadcastClients := make([]BroadcastClient, 500)

	for i := range clients {
		clients[i] = newMockClient()
		registry.addClient(uint32(i+1), clients[i])
		broadcastClients[i] = BroadcastClient{ID: uint32(i + 1), Tag: 0}
	}

	broadcastMsg := createBroadcastMessage(
		broadcastClients,
		[]byte(`{"type":"shared_view_update","data":{"x":100,"y":200}}`),
		BroadcastFlagReliable,
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	// Verify all 500 clients received the same message
	for i, client := range clients {
		msgs := client.getMessages()
		if len(msgs) != 1 {
			t.Errorf("Client %d: expected 1 message, got %d", i+1, len(msgs))
		}
	}
}

func TestBroadcastCompressed(t *testing.T) {
	registry := newMockClientRegistry()
	client1 := newMockClient()
	registry.addClient(1, client1)

	// Create a message and compress it
	originalMessage := []byte(`{"type":"compressed_broadcast","data":"hello"}`)
	encoder, _ := zstd.NewWriter(nil)
	compressedMessage := encoder.EncodeAll(originalMessage, nil)

	broadcastMsg := createBroadcastMessage(
		[]BroadcastClient{{ID: 1, Tag: 0}},
		compressedMessage,
		BroadcastFlagReliable|BroadcastFlagCompressed,
	)

	listener, cleanup, _ := mockBackendServer(t, 1, [][]byte{broadcastMsg})
	defer cleanup()

	pool := NewPool(1)
	pool.SetClientRegistry(registry)
	defer pool.Close()

	err := pool.AddBackend("test-server", listener.Addr().String())
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client should receive the DECOMPRESSED message
	msgs := client1.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if !bytes.Equal(msgs[0].payload, originalMessage) {
		t.Errorf("Expected decompressed message %q, got %q", originalMessage, msgs[0].payload)
	}
}

// Benchmark for BROADCAST fan-out
func BenchmarkBroadcastDispatch(b *testing.B) {
	registry := newMockClientRegistry()
	for i := 0; i < 1000; i++ {
		registry.addClient(uint32(i+1), newMockClient())
	}

	// Create broadcast payload
	clients := make([]BroadcastClient, 1000)
	for i := range clients {
		clients[i] = BroadcastClient{ID: uint32(i + 1), Tag: 0}
	}
	message := []byte(`{"type":"update","path":"/users/alice","data":{"name":"Alice","score":100}}`)

	payloadSize := 4 + len(clients)*8 + 4 + len(message)
	payload := make([]byte, payloadSize)
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(clients)))
	offset := 4
	for _, c := range clients {
		binary.BigEndian.PutUint32(payload[offset:], c.ID)
		binary.BigEndian.PutUint32(payload[offset+4:], uint32(c.Tag))
		offset += 8
	}
	binary.BigEndian.PutUint32(payload[offset:], uint32(len(message)))
	copy(payload[offset+4:], message)

	// Build wire message
	wireData := make([]byte, 2+len(payload))
	wireData[0] = MsgTypeBroadcast
	wireData[1] = BroadcastFlagReliable
	copy(wireData[2:], payload)

	pool := NewPool(1)
	pool.SetClientRegistry(registry)

	backend := &Backend{
		ServerID: "bench",
		pool:     pool,
	}

	conn := &Conn{
		backend: backend,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn.handleBroadcast(wireData)
	}
}

// Benchmark for BROADCAST with tag insertion (Lark format)
func BenchmarkBroadcastDispatchWithTagsLark(b *testing.B) {
	registry := newMockClientRegistry()
	for i := 0; i < 1000; i++ {
		registry.addClient(uint32(i+1), newMockClient())
	}

	// Create broadcast payload with tags for all clients
	clients := make([]BroadcastClient, 1000)
	for i := range clients {
		clients[i] = BroadcastClient{ID: uint32(i + 1), Tag: int32(i + 1)} // Each client has unique tag
	}
	message := []byte(`{"o":"d","p":"/users/alice","v":{"name":"Alice","score":100}}`)

	payloadSize := 4 + len(clients)*8 + 4 + len(message)
	payload := make([]byte, payloadSize)
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(clients)))
	offset := 4
	for _, c := range clients {
		binary.BigEndian.PutUint32(payload[offset:], c.ID)
		binary.BigEndian.PutUint32(payload[offset+4:], uint32(c.Tag))
		offset += 8
	}
	binary.BigEndian.PutUint32(payload[offset:], uint32(len(message)))
	copy(payload[offset+4:], message)

	// Build wire message (Lark format - no FIREBASE flag)
	wireData := make([]byte, 2+len(payload))
	wireData[0] = MsgTypeBroadcast
	wireData[1] = BroadcastFlagReliable
	copy(wireData[2:], payload)

	pool := NewPool(1)
	pool.SetClientRegistry(registry)

	backend := &Backend{
		ServerID: "bench",
		pool:     pool,
	}

	conn := &Conn{
		backend: backend,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn.handleBroadcast(wireData)
	}
}

// Benchmark for BROADCAST with tag insertion (Firebase format)
func BenchmarkBroadcastDispatchWithTagsFirebase(b *testing.B) {
	registry := newMockClientRegistry()
	for i := 0; i < 1000; i++ {
		registry.addClient(uint32(i+1), newMockClient())
	}

	// Create broadcast payload with tags for all clients
	clients := make([]BroadcastClient, 1000)
	for i := range clients {
		clients[i] = BroadcastClient{ID: uint32(i + 1), Tag: int32(i + 1)} // Each client has unique tag
	}
	message := []byte(`{"t":"d","d":{"a":"d","b":{"p":"/users/alice","d":{"name":"Alice","score":100}}}}`)

	payloadSize := 4 + len(clients)*8 + 4 + len(message)
	payload := make([]byte, payloadSize)
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(clients)))
	offset := 4
	for _, c := range clients {
		binary.BigEndian.PutUint32(payload[offset:], c.ID)
		binary.BigEndian.PutUint32(payload[offset+4:], uint32(c.Tag))
		offset += 8
	}
	binary.BigEndian.PutUint32(payload[offset:], uint32(len(message)))
	copy(payload[offset+4:], message)

	// Build wire message (Firebase format)
	wireData := make([]byte, 2+len(payload))
	wireData[0] = MsgTypeBroadcast
	wireData[1] = BroadcastFlagReliable | BroadcastFlagFirebase
	copy(wireData[2:], payload)

	pool := NewPool(1)
	pool.SetClientRegistry(registry)

	backend := &Backend{
		ServerID: "bench",
		pool:     pool,
	}

	conn := &Conn{
		backend: backend,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn.handleBroadcast(wireData)
	}
}

// Benchmark for COMPRESSED_MULTI decompression and dispatch
func BenchmarkCompressedMultiDispatch(b *testing.B) {
	registry := newMockClientRegistry()
	for i := 0; i < 1000; i++ {
		registry.addClient(uint32(i+1), newMockClient())
	}

	// Create a batch of 1000 messages with similar payloads (high compression)
	msgs := make([]CompressedMultiMessage, 1000)
	for i := range msgs {
		msgs[i] = CompressedMultiMessage{
			ClientID: uint32(i + 1),
			Data:     []byte(`{"type":"update","path":"/users/alice","data":{"name":"Alice","score":100}}`),
		}
	}

	// Encode and compress once
	innerSize := 4
	for _, m := range msgs {
		innerSize += 8 + len(m.Data)
	}

	inner := make([]byte, innerSize)
	binary.BigEndian.PutUint32(inner[0:4], uint32(len(msgs)))
	offset := 4
	for _, m := range msgs {
		binary.BigEndian.PutUint32(inner[offset:], m.ClientID)
		binary.BigEndian.PutUint32(inner[offset+4:], uint32(len(m.Data)))
		copy(inner[offset+8:], m.Data)
		offset += 8 + len(m.Data)
	}

	encoder, _ := zstd.NewWriter(nil)
	compressed := encoder.EncodeAll(inner, nil)

	// Create mock pool and backend for the Conn
	pool := NewPool(1)
	pool.SetClientRegistry(registry)

	backend := &Backend{
		ServerID: "bench",
		pool:     pool,
	}

	conn := &Conn{
		backend: backend,
	}

	// Build wire message data (what would come after the length prefix)
	wireData := make([]byte, 2+len(compressed))
	wireData[0] = MsgTypeCompressedMulti
	wireData[1] = FlagReliable | FlagCompressed
	copy(wireData[2:], compressed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn.handleCompressedMulti(wireData)
	}
}
