package backend

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// readFrame reads and discards one length-prefixed wire frame (used by the mock
// servers to consume the HELLO_AUTH the pool sends after HELLO_ACK).
func readFrame(c net.Conn) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c, lenBuf); err != nil {
		return
	}
	n := binary.BigEndian.Uint32(lenBuf)
	if n == 0 || n > 1<<20 {
		return
	}
	_, _ = io.ReadFull(c, make([]byte, n))
}

func TestNewPool(t *testing.T) {
	pool := NewPool(2, "test-secret")
	if pool == nil {
		t.Fatal("NewPool returned nil")
	}
	if pool.backends == nil {
		t.Error("backends map should be initialized")
	}
	if pool.connsPerCore != 2 {
		t.Errorf("connsPerCore: got %d, want 2", pool.connsPerCore)
	}
	defer pool.Close()
}

func TestNewPoolDefaultConnsPerCore(t *testing.T) {
	pool := NewPool(0, "test-secret") // Should default to 2
	if pool.connsPerCore != 2 {
		t.Errorf("connsPerCore should default to 2, got %d", pool.connsPerCore)
	}
	defer pool.Close()
}

func TestPoolAddBackendFailsOnBadAddress(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	// Try to add backend with invalid address
	err := pool.AddBackend("test-server", "invalid:99999")
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestPoolGetBackendNotFound(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	_, err := pool.GetBackend("nonexistent")
	if err != ErrNoConnections {
		t.Errorf("expected ErrNoConnections, got %v", err)
	}
}

func TestPoolClose(t *testing.T) {
	pool := NewPool(2, "test-secret")
	pool.Close()

	// Operations after close should fail
	err := pool.AddBackend("test", "localhost:8080")
	if err != ErrPoolClosed {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}

	_, err = pool.GetBackend("test")
	if err != ErrPoolClosed {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPoolRemoveBackend(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	// Remove non-existent backend should not panic
	pool.RemoveBackend("nonexistent")
}

// mockServer creates a mock server that responds to HELLO with HELLO_ACK
func mockServer(t *testing.T, nrCores uint8) (net.Listener, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	coreCounter := uint8(0)
	done := make(chan struct{})

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
			// Format: [Length:4][Type:1][CoreID:1][NrCores:1][ServerVersion:2][Nonce:32]
			resp := make([]byte, 41)
			binary.BigEndian.PutUint32(resp[0:4], 37) // length = 1+1+1+2+32
			resp[4] = MsgTypeHelloAck
			resp[5] = coreCounter % nrCores          // CoreID (round-robin)
			resp[6] = nrCores                        // NrCores
			binary.BigEndian.PutUint16(resp[7:9], 1) // ServerVersion
			// resp[9:41] nonce (zero is fine; this mock doesn't verify the auth reply)

			coreCounter++
			conn.Write(resp)

			// Consume the HELLO_AUTH the pool sends back so it doesn't pollute
			// any subsequent reads on this connection.
			readFrame(conn)

			// Keep connection open until done
			go func(c net.Conn) {
				<-done
				c.Close()
			}(conn)
		}
	}()

	cleanup := func() {
		close(done)
		listener.Close()
	}

	return listener, cleanup
}

func TestPoolWithMockBackend(t *testing.T) {
	listener, cleanup := mockServer(t, 2)
	defer cleanup()

	pool := NewPool(1, "test-secret") // 1 connection per core
	defer pool.Close()

	addr := listener.Addr().String()
	err := pool.AddBackend("test-server", addr)
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	// Get the backend
	backend, err := pool.GetBackend("test-server")
	if err != nil {
		t.Fatalf("GetBackend failed: %v", err)
	}

	if backend.ServerID != "test-server" {
		t.Errorf("ServerID: got %q, want %q", backend.ServerID, "test-server")
	}

	if backend.Address != addr {
		t.Errorf("Address: got %q, want %q", backend.Address, addr)
	}

	if backend.nrCores != 2 {
		t.Errorf("nrCores: got %d, want 2", backend.nrCores)
	}

	// Remove the backend
	pool.RemoveBackend("test-server")

	_, err = pool.GetBackend("test-server")
	if err != ErrNoConnections {
		t.Errorf("After remove, expected ErrNoConnections, got %v", err)
	}
}

func TestGetOrCreateBackendNew(t *testing.T) {
	listener, cleanup := mockServer(t, 2)
	defer cleanup()

	pool := NewPool(1, "test-secret")
	defer pool.Close()

	addr := listener.Addr().String()

	// First call should create
	backend1, err := pool.GetOrCreateBackend("new-server", addr)
	if err != nil {
		t.Fatalf("GetOrCreateBackend failed: %v", err)
	}

	// Second call should return existing
	backend2, err := pool.GetOrCreateBackend("new-server", addr)
	if err != nil {
		t.Fatalf("GetOrCreateBackend (2nd) failed: %v", err)
	}

	if backend1 != backend2 {
		t.Error("Expected same backend instance")
	}
}

func TestBackendMultipleConnectionsPerCore(t *testing.T) {
	listener, cleanup := mockServer(t, 2)
	defer cleanup()

	pool := NewPool(2, "test-secret") // 2 connections per core
	defer pool.Close()

	addr := listener.Addr().String()
	err := pool.AddBackend("test-server", addr)
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	backend, _ := pool.GetBackend("test-server")

	// Verify backend has correct number of cores and connections
	backend.mu.Lock()
	nrCores := backend.nrCores
	totalConns := 0
	for _, coreConns := range backend.coreConns {
		totalConns += len(coreConns)
	}
	backend.mu.Unlock()

	if nrCores != 2 {
		t.Errorf("Expected 2 cores, got %d", nrCores)
	}

	// Should have 2 connections per core * 2 cores = 4 total
	if totalConns != 4 {
		t.Errorf("Expected 4 total connections, got %d", totalConns)
	}
}

func TestPoolConcurrentAccess(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	// Concurrent access should not panic
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				pool.GetBackend("nonexistent")
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestBackendRegisterClient(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	// Create a minimal backend for testing
	backend := &Backend{
		ServerID:     "test",
		Address:      "invalid:99999",
		nrCores:      4,
		coreConns:    make([][]*Conn, 4),
		clientToCore: make(map[uint32]int),
		pool:         pool,
		inbox:        make(chan *inboxMessage, 10),
		done:         make(chan struct{}),
	}

	// Register a client
	coreID := backend.RegisterClient(1, "test-project", "test-database")

	// Verify the client is mapped to a core
	backend.mu.RLock()
	mappedCore, ok := backend.clientToCore[1]
	backend.mu.RUnlock()

	if !ok {
		t.Error("Client should be registered")
	}
	if mappedCore != coreID {
		t.Errorf("Mapped core mismatch: got %d, returned %d", mappedCore, coreID)
	}

	// Unregister
	backend.UnregisterClient(1)

	backend.mu.RLock()
	_, ok = backend.clientToCore[1]
	backend.mu.RUnlock()

	if ok {
		t.Error("Client should be unregistered")
	}
}

func TestBackendSendMessageRequiresRegistration(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	backend := &Backend{
		ServerID:     "test",
		Address:      "invalid:99999",
		nrCores:      4,
		coreConns:    make([][]*Conn, 4),
		clientToCore: make(map[uint32]int),
		pool:         pool,
		inbox:        make(chan *inboxMessage, 10),
		done:         make(chan struct{}),
	}

	msg := &Message{
		Type:     MsgTypeData,
		ClientID: 1,
		Payload:  []byte("test"),
	}

	// SendMessage without registration should fail
	err := backend.SendMessage(msg)
	if err != ErrClientNotRouted {
		t.Errorf("Expected ErrClientNotRouted, got %v", err)
	}

	// Register the client
	backend.RegisterClient(1, "test-project", "test-database")

	// Now SendMessage should succeed
	err = backend.SendMessage(msg)
	if err != nil {
		t.Errorf("SendMessage should succeed after registration, got %v", err)
	}

	// Message should be in inbox with correct core
	select {
	case received := <-backend.inbox:
		if received.msg.ClientID != 1 {
			t.Errorf("Wrong client ID in inbox")
		}
	default:
		t.Error("Message not found in inbox")
	}
}

func TestBackendSendConnect(t *testing.T) {
	pool := NewPool(2, "test-secret")
	defer pool.Close()

	backend := &Backend{
		ServerID:     "test",
		Address:      "invalid:99999",
		nrCores:      4,
		coreConns:    make([][]*Conn, 4),
		clientToCore: make(map[uint32]int),
		pool:         pool,
		inbox:        make(chan *inboxMessage, 10),
		done:         make(chan struct{}),
	}

	payload := []byte("connect-payload")

	// SendConnect should register the client and send the message
	err := backend.SendConnect(1, "test-project", "test-database", payload)
	if err != nil {
		t.Errorf("SendConnect failed: %v", err)
	}

	// Client should now be registered
	backend.mu.RLock()
	_, ok := backend.clientToCore[1]
	backend.mu.RUnlock()

	if !ok {
		t.Error("Client should be registered after SendConnect")
	}

	// Message should be in inbox
	select {
	case received := <-backend.inbox:
		if received.msg.Type != MsgTypeConnect {
			t.Errorf("Expected CONNECT message type, got %d", received.msg.Type)
		}
		if received.msg.ClientID != 1 {
			t.Errorf("Wrong client ID")
		}
	default:
		t.Error("Message not found in inbox")
	}
}

func TestCoreForDatabaseConsistency(t *testing.T) {
	// Test that the same database always maps to the same core
	databaseID := "my-test-database"
	nrCores := 8

	core1 := CoreForDatabase(databaseID, nrCores)
	core2 := CoreForDatabase(databaseID, nrCores)

	if core1 != core2 {
		t.Errorf("CoreForDatabase should be deterministic: got %d and %d", core1, core2)
	}

	// Verify it's in valid range
	if core1 < 0 || core1 >= nrCores {
		t.Errorf("Core %d out of range [0, %d)", core1, nrCores)
	}
}

func TestCoreForDatabaseDistribution(t *testing.T) {
	// Test that databases are distributed across cores
	nrCores := 8
	coreCounts := make([]int, nrCores)

	// Hash 1000 different database IDs
	for i := 0; i < 1000; i++ {
		dbID := string(rune('a'+i%26)) + string(rune('0'+i/26))
		core := CoreForDatabase(dbID, nrCores)
		coreCounts[core]++
	}

	// Check that no core has 0 assignments (very unlikely with good hash)
	for i, count := range coreCounts {
		if count == 0 {
			t.Errorf("Core %d has no assignments - poor distribution", i)
		}
	}
}

func TestBackendNrCores(t *testing.T) {
	listener, cleanup := mockServer(t, 4)
	defer cleanup()

	pool := NewPool(1, "test-secret")
	defer pool.Close()

	addr := listener.Addr().String()
	err := pool.AddBackend("test-server", addr)
	if err != nil {
		t.Fatalf("AddBackend failed: %v", err)
	}

	backend, _ := pool.GetBackend("test-server")

	if backend.NrCores() != 4 {
		t.Errorf("NrCores: got %d, want 4", backend.NrCores())
	}
}

func TestQueueStats(t *testing.T) {
	listener, cleanup := mockServer(t, 2)
	defer cleanup()

	pool := NewPool(1, "test-secret")
	defer pool.Close()

	addr := listener.Addr().String()
	pool.AddBackend("test-server", addr)

	// Wait briefly for goroutines to start
	time.Sleep(10 * time.Millisecond)

	stats := pool.GetQueueStats()
	if _, ok := stats["test-server"]; !ok {
		t.Error("Expected stats for test-server")
	}
}
