package proxy

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/config"
)

// newOutboxTestClient builds a client whose limits come from cfg (so tests can
// use tiny caps) and whose server-wide counter can be inspected.
func newOutboxTestClient(t *testing.T, transport ClientTransport, cfg *config.Config) (*ClientConn, *Server) {
	t.Helper()
	server := &Server{config: cfg}
	client := newTestClient(1, transport, ProtocolLark)
	client.server = server
	return client, server
}

// slowTransport blocks every Send until released, so a queue can build up.
type slowTransport struct {
	*MockTransport
	release chan struct{}
	sends   int
	mu      sync.Mutex
}

func (s *slowTransport) Send(data []byte, reliable bool) error {
	<-s.release
	s.mu.Lock()
	s.sends++
	s.mu.Unlock()
	return s.MockTransport.Send(data, reliable)
}

func TestDeliverRefusesBeyondByteCap(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1000, ClientOutboxWarnBytes: 10000}
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client, server := newOutboxTestClient(t, transport, cfg)
	// No writeLoop running: everything delivered stays queued.

	payload := make([]byte, 200) // 200 + 64 overhead = 264 per message
	for i := 0; i < 3; i++ {
		if !client.Deliver(payload, true) {
			t.Fatalf("delivery %d should fit under the cap", i)
		}
	}
	// 3*264 = 792 queued; a fourth (264) would reach 1056 > 1000.
	if client.Deliver(payload, true) {
		t.Fatal("fourth delivery should be refused: byte cap exceeded")
	}
	// A small message still fits (792 + 64 + 100 = 956).
	if !client.Deliver(make([]byte, 100), true) {
		t.Fatal("small delivery should still fit under the cap")
	}

	queued, peak := client.OutboxBytes()
	if queued != 956 || peak != 956 {
		t.Fatalf("queued=%d peak=%d, want 956/956", queued, peak)
	}
	if got := server.outboxBytesTotal.Load(); got != 956 {
		t.Fatalf("server total=%d, want 956", got)
	}
}

func TestOutboxDrainReleasesBytes(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1 << 20, ClientOutboxWarnBytes: 1 << 20}
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client, server := newOutboxTestClient(t, transport, cfg)

	for i := 0; i < 5; i++ {
		client.Deliver(make([]byte, 100), true)
	}
	if queued, _ := client.OutboxBytes(); queued != 5*164 {
		t.Fatalf("queued=%d, want %d", queued, 5*164)
	}

	for i := 0; i < 5; i++ {
		if msg := client.popOutbox(); msg == nil {
			t.Fatalf("pop %d returned nil", i)
		}
	}
	if msg := client.popOutbox(); msg != nil {
		t.Fatal("queue should be empty")
	}
	queued, peak := client.OutboxBytes()
	if queued != 0 || peak != 5*164 {
		t.Fatalf("queued=%d peak=%d, want 0/%d", queued, peak, 5*164)
	}
	if got := server.outboxBytesTotal.Load(); got != 0 {
		t.Fatalf("server total=%d after drain, want 0", got)
	}
}

func TestOutboxWarnArmsOnceAndRearms(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1 << 20, ClientOutboxWarnBytes: 500}
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client, _ := newOutboxTestClient(t, transport, cfg)

	// Climb past the warn line: 3 * 264 = 792 >= 500.
	for i := 0; i < 3; i++ {
		client.Deliver(make([]byte, 200), true)
	}
	client.outboxMu.Lock()
	warned := client.outboxWarned
	client.outboxMu.Unlock()
	if !warned {
		t.Fatal("warn should be armed after crossing the warn line")
	}

	// Drain below half the warn line (250): pop all three.
	for i := 0; i < 3; i++ {
		client.popOutbox()
	}
	client.outboxMu.Lock()
	warned = client.outboxWarned
	client.outboxMu.Unlock()
	if warned {
		t.Fatal("warn should re-arm once drained below half the warn line")
	}
}

func TestWriteLoopDeliversInOrderThenIdles(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1 << 20, ClientOutboxWarnBytes: 1 << 20}
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client, server := newOutboxTestClient(t, transport, cfg)
	go client.writeLoop()

	for i := 0; i < 50; i++ {
		client.Deliver([]byte{byte(i)}, true)
	}
	deadline := time.Now().Add(2 * time.Second)
	for transport.MessageCount() < 50 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	msgs := transport.SentMessages()
	if len(msgs) != 50 {
		t.Fatalf("sent %d messages, want 50", len(msgs))
	}
	for i, m := range msgs {
		if m.data[0] != byte(i) {
			t.Fatalf("message %d out of order: got %d", i, m.data[0])
		}
	}
	if got := server.outboxBytesTotal.Load(); got != 0 {
		t.Fatalf("server total=%d after drain, want 0", got)
	}
	client.Close()
}

func TestSlowButDrainingClientIsNotKicked(t *testing.T) {
	// Cap holds ~10 messages; the transport drains slowly but steadily.
	cfg := &config.Config{ClientOutboxMaxBytes: 10 * (100 + outboxMsgOverhead), ClientOutboxWarnBytes: 1 << 20}
	slow := &slowTransport{MockTransport: NewMockTransport(backend.ProtocolWebSocket), release: make(chan struct{})}
	client, _ := newOutboxTestClient(t, slow, cfg)
	go client.writeLoop()

	// Producer: 100 messages, one at a time, each only after the previous
	// one has been released to the transport (drain keeping pace).
	for i := 0; i < 100; i++ {
		if !client.Deliver(make([]byte, 100), true) {
			t.Fatalf("delivery %d refused although the client is draining", i)
		}
		slow.release <- struct{}{}
	}
	client.Close()
	if slow.IsClosed() != true {
		t.Fatal("transport should be closed after Close")
	}
}

func TestBurstBeyondCapIsRefused(t *testing.T) {
	// Same cap, but the transport never drains: a burst past the cap must be
	// refused so the caller can Kick.
	cfg := &config.Config{ClientOutboxMaxBytes: 10 * (100 + outboxMsgOverhead), ClientOutboxWarnBytes: 1 << 20}
	slow := &slowTransport{MockTransport: NewMockTransport(backend.ProtocolWebSocket), release: make(chan struct{})}
	client, _ := newOutboxTestClient(t, slow, cfg)
	go client.writeLoop()

	accepted := 0
	for i := 0; i < 100; i++ {
		if client.Deliver(make([]byte, 100), true) {
			accepted++
		}
	}
	// The write loop may have popped one message (releasing its bytes) and be
	// blocked in Send with it, so 10 or 11 fit.
	if accepted < 10 || accepted > 11 {
		t.Fatalf("accepted %d, want 10 or 11", accepted)
	}
	client.Kick("outbox full")
	if client.State() != StateClosing {
		t.Fatal("Kick should close the client")
	}
	close(slow.release) // unblock the write loop so it can exit
}

func TestKickLogsOnceAndCloses(t *testing.T) {
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client := newTestClient(1, transport, ProtocolFirebase)
	client.Kick("test reason", "extra", 1)
	if !transport.IsClosed() {
		t.Fatal("transport should be closed after Kick")
	}
	client.Kick("test reason again") // no-op, must not panic
}

type timeoutTransport struct {
	*MockTransport
}

type fakeTimeout struct{}

func (fakeTimeout) Error() string   { return "i/o timeout" }
func (fakeTimeout) Timeout() bool   { return true }
func (fakeTimeout) Temporary() bool { return false }

func (tt *timeoutTransport) Send(data []byte, reliable bool) error {
	return fakeTimeout{}
}

func TestWriteTimeoutKicksClient(t *testing.T) {
	transport := &timeoutTransport{MockTransport: NewMockTransport(backend.ProtocolWebSocket)}
	client := newTestClient(1, transport, ProtocolLark)
	done := make(chan struct{})
	go func() {
		client.writeLoop()
		close(done)
	}()
	client.Deliver([]byte("x"), true)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop should exit after a write timeout")
	}
	if !transport.IsClosed() {
		t.Fatal("client should be closed after a write timeout")
	}
}

type failingTransport struct {
	*MockTransport
}

func (ft *failingTransport) Send(data []byte, reliable bool) error {
	return errors.New("broken pipe")
}

func TestWriteFailureClosesQuietly(t *testing.T) {
	transport := &failingTransport{MockTransport: NewMockTransport(backend.ProtocolWebSocket)}
	client := newTestClient(1, transport, ProtocolLark)
	done := make(chan struct{})
	go func() {
		client.writeLoop()
		close(done)
	}()
	client.Deliver([]byte("x"), true)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop should exit after a write failure")
	}
	if !transport.IsClosed() {
		t.Fatal("client should be closed after a write failure")
	}
}

func TestWriteDeadlineScalesWithPayload(t *testing.T) {
	cfg := &config.Config{ClientWriteDeadline: 10 * time.Second, ClientWriteMinBytesPerSec: 64 * 1024}
	client, _ := newOutboxTestClient(t, NewMockTransport(backend.ProtocolWebSocket), cfg)

	cases := []struct {
		payload int
		want    time.Duration
	}{
		{0, 10 * time.Second},
		{16 * 1024, 10*time.Second + 250*time.Millisecond},
		{16 * 1024 * 1024, 10*time.Second + 256*time.Second},
	}
	for _, c := range cases {
		if got := client.WriteDeadline(c.payload); got != c.want {
			t.Errorf("WriteDeadline(%d) = %v, want %v", c.payload, got, c.want)
		}
	}

	// A zero floor disables the scaling.
	cfg.ClientWriteMinBytesPerSec = 0
	if got := client.WriteDeadline(1 << 30); got != 10*time.Second {
		t.Errorf("WriteDeadline with zero min rate = %v, want 10s", got)
	}

	// No server: defaults apply.
	bare := newTestClient(2, NewMockTransport(backend.ProtocolWebSocket), ProtocolLark)
	if got := bare.WriteDeadline(0); got != defaultClientWriteDeadline {
		t.Errorf("default WriteDeadline = %v, want %v", got, defaultClientWriteDeadline)
	}
}

func TestCloseReleasesQueuedBytesFromServerTotal(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1 << 20, ClientOutboxWarnBytes: 1 << 20}
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client, server := newOutboxTestClient(t, transport, cfg)
	for i := 0; i < 5; i++ {
		client.Deliver(make([]byte, 100), true)
	}
	if got := server.outboxBytesTotal.Load(); got == 0 {
		t.Fatal("server total should reflect queued bytes")
	}
	client.Close()
	if got := server.outboxBytesTotal.Load(); got != 0 {
		t.Fatalf("server total=%d after Close, want 0", got)
	}
	if queued, _ := client.OutboxBytes(); queued != 0 {
		t.Fatalf("queued=%d after Close, want 0", queued)
	}
}

func TestOutboxCompactsDeadPrefix(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1 << 30, ClientOutboxWarnBytes: 1 << 30}
	client, _ := newOutboxTestClient(t, NewMockTransport(backend.ProtocolWebSocket), cfg)
	for i := 0; i < 3000; i++ {
		client.Deliver([]byte{1}, true)
	}
	for i := 0; i < 2000; i++ {
		client.popOutbox()
	}
	// Compaction slides the live tail down once the dead prefix reaches
	// half the slice (at head=1500 of 3000 here), so the backing array never
	// keeps growing for a long-lived slow client.
	client.outboxMu.Lock()
	head, n := client.outboxHead, len(client.outboxQ)
	client.outboxMu.Unlock()
	if n-head != 1000 {
		t.Fatalf("live messages=%d after 2000 pops, want 1000", n-head)
	}
	if head != 500 || n != 1500 {
		t.Fatalf("head=%d len=%d, want 500/1500 (compacted once at head=1500)", head, n)
	}
	for i := 0; i < 1000; i++ {
		if client.popOutbox() == nil {
			t.Fatalf("pop %d after compaction returned nil", i)
		}
	}
	if client.popOutbox() != nil {
		t.Fatal("queue should be empty after draining")
	}
}

func TestDeliverAfterCloseIsRefused(t *testing.T) {
	cfg := &config.Config{ClientOutboxMaxBytes: 1 << 20, ClientOutboxWarnBytes: 1 << 20}
	transport := NewMockTransport(backend.ProtocolWebSocket)
	client, server := newOutboxTestClient(t, transport, cfg)
	client.Close()
	if client.Deliver(make([]byte, 100), true) {
		t.Fatal("Deliver after Close should be refused")
	}
	if got := server.outboxBytesTotal.Load(); got != 0 {
		t.Fatalf("server total=%d after Deliver-after-Close, want 0", got)
	}
}
