// WebTransport Implementation
//
// This file implements the WebTransport transport for client connections.
// WebTransport is a modern protocol built on QUIC/HTTP3 that provides:
//   - Lower latency than WebSocket (0-RTT connection establishment)
//   - Unreliable datagrams (for real-time data like player positions)
//   - Multiple streams (not currently used)
//
// # Protocol Modes
//
// The transport supports two message delivery modes:
//   - Reliable: Sent over the bidirectional stream (guaranteed delivery)
//   - Unreliable: Sent as QUIC datagrams (may be dropped, lower latency)
//
// The reliable flag is determined by the backend and passed through Deliver().
// Volatile data (marked with .volatile in security rules) uses datagrams.
//
// # Message Framing
//
// Unlike WebSocket which has built-in framing, WebTransport streams need
// explicit length prefixes. Messages are framed as:
//
//	┌──────────────┬─────────────────┐
//	│ Length (4B)  │ Payload         │
//	│ big-endian   │ (variable)      │
//	└──────────────┴─────────────────┘
//
// Datagrams don't need framing (each datagram is a complete message).
//
// # Multiple Ports
//
// WebTransport can run on multiple UDP ports (configurable via WEBTRANSPORT_PORTS).
// This allows load distribution across CPU cores since each port is handled
// by a separate goroutine. The port number is tracked per connection.
//
// # Thread Safety
//
// Similar to WebSocket, writes are protected by writeMu mutex. The session
// and stream are thread-safe for concurrent reads/writes.
package proxy

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/logger"

	"github.com/quic-go/webtransport-go"
)

// WebTransportConn handles a WebTransport client connection
type WebTransportConn struct {
	session *webtransport.Session
	stream  *webtransport.Stream
	client  *ClientConn
	port    int

	writeMu sync.Mutex
	closed  bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewWebTransportConn creates a new WebTransport connection
func NewWebTransportConn(session *webtransport.Session, stream *webtransport.Stream, port int) *WebTransportConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &WebTransportConn{
		session: session,
		stream:  stream,
		port:    port,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// SetClient sets the client connection
func (t *WebTransportConn) SetClient(client *ClientConn) {
	t.client = client
}

// Send sends data to the client
func (t *WebTransportConn) Send(data []byte, reliable bool) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if t.closed {
		return nil
	}

	if reliable {
		// Send via stream with length prefix
		return t.sendReliable(data)
	} else {
		// Send via datagram (unreliable)
		return t.sendDatagram(data)
	}
}

// sendReliable sends data reliably via the bidirectional stream
func (t *WebTransportConn) sendReliable(data []byte) error {
	// 4-byte big-endian length prefix
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	t.stream.SetWriteDeadline(time.Now().Add(10 * time.Second))

	if _, err := t.stream.Write(lenBuf); err != nil {
		return err
	}
	if _, err := t.stream.Write(data); err != nil {
		return err
	}
	return nil
}

// sendDatagram sends data unreliably via QUIC datagram
func (t *WebTransportConn) sendDatagram(data []byte) error {
	return t.session.SendDatagram(data)
}

// Close closes the connection
func (t *WebTransportConn) Close() error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true
	t.cancel()

	t.stream.Close()
	return t.session.CloseWithError(0, "closed")
}

// TransportType returns the transport protocol type
func (t *WebTransportConn) TransportType() byte {
	return backend.ProtocolWebTransport
}

// ReadLoop reads messages from the stream (reliable)
func (t *WebTransportConn) ReadLoop() {
	defer t.client.Close()

	lenBuf := make([]byte, 4)

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		// Read length prefix
		t.stream.SetReadDeadline(time.Now().Add(60 * time.Second))
		if _, err := io.ReadFull(t.stream, lenBuf); err != nil {
			if err != io.EOF {
				logger.Debug("WT read length error", "client_id", t.client.id, "error", err)
			}
			return
		}

		length := binary.BigEndian.Uint32(lenBuf)
		if length > 16*1024*1024 { // 16MB max
			logger.Warn("WT message too large", "client_id", t.client.id, "length", length)
			return
		}

		// Read message
		data := make([]byte, length)
		if _, err := io.ReadFull(t.stream, data); err != nil {
			logger.Debug("WT read data error", "client_id", t.client.id, "error", err)
			return
		}

		t.client.OnMessage(data, true) // Reliable message
	}
}

// DatagramReadLoop reads datagrams (unreliable)
func (t *WebTransportConn) DatagramReadLoop() {
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		data, err := t.session.ReceiveDatagram(t.ctx)
		if err != nil {
			if t.ctx.Err() == nil {
				logger.Debug("WT datagram read error", "client_id", t.client.id, "error", err)
			}
			return
		}

		t.client.OnMessage(data, false) // Unreliable message
	}
}

// handleWebTransportSession handles a WebTransport session after upgrade
func (s *Server) handleWebTransportSession(session *webtransport.Session, port int, projectID string, subdomainDB string) {
	// Accept bidirectional stream for reliable messages
	acceptCtx, acceptCancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream, err := session.AcceptStream(acceptCtx)
	acceptCancel()
	if err != nil {
		logger.Warn("WT accept stream error", "error", err, "remote", session.RemoteAddr())
		session.CloseWithError(1, "stream error")
		return
	}

	transport := NewWebTransportConn(session, stream, port)
	clientID := s.allocateClientID()
	if clientID == 0 {
		logger.Warn("WT connection rejected: too many concurrent connections")
		session.CloseWithError(1, "too many connections")
		return
	}
	client := newClientConn(s, clientID, transport, ProtocolLark)
	transport.SetClient(client)

	// Set project ID from request host (extracted before upgrade)
	client.projectID = projectID
	if subdomainDB != "" {
		client.databaseID = subdomainDB
	}

	// Start fetching project config immediately (async)
	client.fetchProjectConfig()

	s.registerClient(client)

	// Start client's write goroutine
	client.Start()

	logger.Debug("WT new connection", "client_id", clientID, "remote", session.RemoteAddr(), "port", port, "project_id", projectID)

	// Start read loops
	go transport.DatagramReadLoop()
	transport.ReadLoop() // Blocks until connection closes
}
