// Connection Management
//
// This file implements single TCP connections to backend servers. Each Conn handles:
//   - Writing batched messages to the backend (thread-safe via mutex)
//   - Reading responses and routing them to appropriate handlers
//   - Connection health monitoring and automatic reconnection (via Backend)
//
// # Connection Lifecycle
//
// 1. Pool.AddBackend() creates Backend with N connections per core
// 2. Backend.connectCore() dials TCP and creates Conn
// 3. Backend starts Conn.readLoop() in a goroutine
// 4. Conn reads messages until connection closes or errors
// 5. On close, Backend.handleConnDeath() is called to trigger reconnection
//
// # Write Path
//
// Writes go through the Backend's batching system, not directly to Conn:
// 1. Backend.flushBatch() collects messages for each core
// 2. For each connection, calls conn.WriteRaw() with the batch
// 3. WriteRaw() is protected by a mutex for concurrent access
// 4. 64KB buffered writer reduces syscalls
//
// # Read Path (High-Throughput Design)
//
// Each Conn has a dedicated readLoop goroutine optimized for 500MB/sec+ throughput:
// 1. Reads directly from TCP socket (no bufio overhead)
// 2. Batch-processes all complete messages from each read in a tight loop
// 3. Single syscall can yield many messages
// 4. Large messages (>8MB) get dedicated allocation and direct read
// 5. Client dispatch is done inline (lookup + deliver) for minimal latency
//
// # COMPRESSED_MULTI Support
//
// For SharedView fan-out, the backend can send a single COMPRESSED_MULTI message
// containing batched messages for multiple clients. This reduces bandwidth by
// 95%+ for nearly-identical payloads and reduces syscalls from N to 1.
// The proxy decompresses (ZSTD) and dispatches to individual clients.
//
// # Buffer Sizes
//
// - TCP read buffer: 4MB kernel buffer for burst absorption
// - Read loop buffer: 8MB userspace buffer for batch processing
// - Write buffer: 64KB (batches small writes)
// - Response channel: 100k messages per shard
package backend

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"github.com/lark-sh/lark/edge/logger"
)

// Package-level ZSTD decoder for decompressing COMPRESSED_MULTI messages.
// Thread-safe and reusable across all connections.
var zstdDecoder *zstd.Decoder

func init() {
	var err error
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("failed to create zstd decoder: " + err.Error())
	}
}

// Conn represents a single connection to a backend server
type Conn struct {
	conn    net.Conn
	backend *Backend
	coreID  int // The core this connection is assigned to

	// Buffered writer with mutex for concurrent access
	writer   *bufio.Writer
	writerMu sync.Mutex

	// State
	closed atomic.Bool
}

// NewConn creates a new backend connection assigned to a specific core
func NewConn(conn net.Conn, backend *Backend, coreID int) *Conn {
	// Set large kernel read buffer for burst absorption
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetReadBuffer(4 * 1024 * 1024) // 4MB kernel read buffer
	}

	c := &Conn{
		conn:    conn,
		backend: backend,
		coreID:  coreID,
		writer:  bufio.NewWriterSize(conn, 65536), // 64KB buffer
	}
	return c
}

// isControlMessage returns true if the message type is a coordinator control message (no ClientID)
func isControlMessage(msgType byte) bool {
	switch msgType {
	case MsgTypeHeartbeat, MsgTypeDatabaseLoaded, MsgTypeDatabaseUnloaded, MsgTypeConfigRequest:
		return true
	default:
		return false
	}
}

// readLoop reads messages from the backend and routes them appropriately.
//
// HOT PATH: Optimized for 500MB/sec+ throughput per connection.
//
// Key optimizations:
//   - Direct TCP reads (no bufio layer)
//   - Batch processing: one syscall yields many messages
//   - Tight inner loop processes all complete messages before next read
//   - Large messages (>8MB) handled separately with dedicated allocation
//   - Inline dispatch: client lookup + deliver happens directly, no channel hops
//
// Message routing:
//   - Client messages (DATA, CLOSE): Dispatched inline to client
//   - COMPRESSED_MULTI: Decompressed and dispatched to multiple clients
//   - Control messages (HEARTBEAT, etc.): Go to control channel for special handling
//
// Per-client message ordering is guaranteed because a client's messages always arrive
// on the same TCP connection and are processed sequentially by this readLoop.
//
// On error, the connection is closed and handleConnDeath is called, which may trigger
// reconnection on the next flush or client notifications if all connections are dead.
func (c *Conn) readLoop() {
	defer c.backend.handleConnDeath(c)
	defer c.Close()

	// Buffer for batch reading - 8MB handles most messages efficiently
	// Messages larger than this get a dedicated allocation
	const bufSize = 8 * 1024 * 1024
	buf := make([]byte, bufSize)
	offset := 0 // Valid data in buf is buf[0:offset]

	for {
		// Read as much as available from TCP
		// This may return anywhere from 1 byte to bufSize-offset bytes
		n, err := c.conn.Read(buf[offset:])
		if err != nil {
			if !c.closed.Load() {
				logger.Warn("Read error", "server_id", c.backend.ServerID, "error", err)
			}
			return
		}
		offset += n

		// Process all complete messages in buffer
		// This is the hot loop - processes many messages per syscall
		pos := 0
	processLoop:
		for {
			remaining := offset - pos

			// Need at least 4 bytes for length prefix
			if remaining < 4 {
				break
			}

			// Parse message length
			msgLen := int(binary.BigEndian.Uint32(buf[pos:]))
			totalLen := 4 + msgLen

			// Validate message length
			if msgLen < 1 {
				logger.Warn("Invalid message length, skipping", "server_id", c.backend.ServerID, "length", msgLen)
				pos += 4 // Skip just the length field, try to resync
				continue
			}
			if msgLen > MaxMessageSize+5 {
				logger.Warn("Message exceeds max size", "server_id", c.backend.ServerID, "length", msgLen)
				return // Protocol error - close connection
			}

			// Check if this message is too large to safely accumulate in our buffer.
			// Use bufSize-1MB threshold (matching the safety check below) so that
			// messages between 7-8MB get a dedicated allocation instead of slowly
			// filling the buffer until the safety check kills the connection.
			if totalLen > bufSize-1024*1024 {
				// Large message - allocate dedicated buffer and read directly
				// We have the 4-byte length and possibly some payload bytes
				largeData := make([]byte, msgLen)

				// Copy any payload bytes we already have in the buffer
				payloadInBuf := remaining - 4
				if payloadInBuf > 0 {
					copy(largeData, buf[pos+4:offset])
				}

				// Read the rest directly from the connection
				if payloadInBuf < msgLen {
					_, err := io.ReadFull(c.conn, largeData[payloadInBuf:])
					if err != nil {
						if !c.closed.Load() {
							logger.Warn("Read error (large message)", "server_id", c.backend.ServerID, "error", err)
						}
						return
					}
				}

				// Process the large message
				c.processMessage(largeData)

				// Reset buffer - we've consumed everything
				offset = 0
				pos = 0
				break processLoop
			}

			// Normal case - need full message in buffer
			if remaining < totalLen {
				break
			}

			// Process message - data is buf[pos+4 : pos+totalLen]
			c.processMessage(buf[pos+4 : pos+totalLen])
			pos += totalLen
		}

		// Shift unprocessed data to front of buffer
		if pos > 0 {
			remaining := offset - pos
			if remaining > 0 {
				copy(buf, buf[pos:offset])
			}
			offset = remaining
		}

		// Safety check: buffer nearly full but no complete message
		// This can happen if we have a partial length prefix or partial message
		// that's smaller than bufSize. Should resolve on next read.
		if offset > bufSize-1024*1024 {
			logger.Error("Read buffer nearly full with incomplete message", "server_id", c.backend.ServerID, "offset", offset)
			return
		}
	}
}

// processMessage handles a single message from the read buffer.
// Called from readLoop's tight inner loop - must be fast.
func (c *Conn) processMessage(data []byte) {
	if len(data) < 1 {
		return
	}

	msgType := data[0]

	if isControlMessage(msgType) {
		// Control message - no ClientID, route to control channel
		// Control messages are infrequent, so allocation here is fine
		var payload []byte
		if len(data) > 1 {
			payload = make([]byte, len(data)-1)
			copy(payload, data[1:])
		}
		controlMsg := &ControlMessage{
			Type:    msgType,
			Payload: payload,
		}
		c.backend.EnqueueControlMessage(c.coreID, controlMsg)
	} else if msgType == MsgTypeCompressedMulti {
		// Compressed batch of messages for multiple clients
		c.handleCompressedMulti(data)
	} else if msgType == MsgTypeBroadcast {
		// Broadcast same message to multiple clients
		c.handleBroadcast(data)
	} else {
		// Client message - dispatch inline
		if len(data) < 5 {
			logger.Warn("Client message too short", "server_id", c.backend.ServerID, "length", len(data))
			return
		}

		clientID := binary.BigEndian.Uint32(data[1:5])

		// Must copy payload - client.Deliver() queues to outbox channel,
		// and the read buffer will be reused before the message is sent
		var payload []byte
		if len(data) > 5 {
			payload = make([]byte, len(data)-5)
			copy(payload, data[5:])
		}

		c.dispatchClientMessage(msgType, clientID, payload)
	}
}

// handleCompressedMulti decompresses and dispatches a batch of messages.
// Format: [Type:1=0x0A][Flags:1][CompressedPayload:var]
// Inner format after decompression: [Count:4][[ClientID:4][Len:4][Data:var]]...
func (c *Conn) handleCompressedMulti(data []byte) {
	if len(data) < 2 {
		logger.Warn("COMPRESSED_MULTI too short", "server_id", c.backend.ServerID, "length", len(data))
		return
	}

	flags := data[1]
	compressed := data[2:]

	// Decompress
	decompressed, err := zstdDecoder.DecodeAll(compressed, nil)
	if err != nil {
		logger.Error("Failed to decompress COMPRESSED_MULTI", "server_id", c.backend.ServerID, "error", err)
		return
	}

	// Parse inner format and dispatch each message
	messages, err := DecodeCompressedMultiPayload(decompressed)
	if err != nil {
		logger.Error("Failed to decode COMPRESSED_MULTI payload", "server_id", c.backend.ServerID, "error", err)
		return
	}

	reliable := flags&FlagReliable != 0

	for _, msg := range messages {
		c.deliverToClient(msg.ClientID, msg.Data, reliable)
	}
}

// insertTagFirebase inserts a tag into a Firebase-format message.
// Firebase messages end with "}}}" and we insert ,"t":TAG before the final }}}.
// If the message doesn't end with "}}}", it's returned unchanged.
func insertTagFirebase(msg []byte, tag int32) []byte {
	// Verify message ends with "}}}"
	if len(msg) < 3 || string(msg[len(msg)-3:]) != "}}}" {
		return msg // Unexpected format, return unchanged
	}

	tagStr := fmt.Sprintf(`,"t":%d`, tag)

	result := make([]byte, 0, len(msg)+len(tagStr))
	result = append(result, msg[:len(msg)-3]...) // Everything before }}}
	result = append(result, tagStr...)           // ,"t":42
	result = append(result, "}}}"...)            // }}}
	return result
}

// insertTagLark inserts a tag into a Lark-format message.
// Lark messages start with "{" and we insert "tag":TAG, after the opening {.
// If the message doesn't start with "{", it's returned unchanged.
func insertTagLark(msg []byte, tag int32) []byte {
	// Verify message starts with "{"
	if len(msg) < 1 || msg[0] != '{' {
		return msg // Unexpected format, return unchanged
	}

	prefix := fmt.Sprintf(`{"tag":%d,`, tag)

	result := make([]byte, 0, len(msg)-1+len(prefix))
	result = append(result, prefix...)  // {"tag":42,
	result = append(result, msg[1:]...) // o":"d","p":...} (skip original {)
	return result
}

// handleBroadcast broadcasts the same message to multiple clients.
// Format: [Type:1=0x0B][Flags:1][ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes:var]
//
// Flags:
//   - 0x01: RELIABLE - use reliable delivery
//   - 0x02: FIREBASE - message is Firebase format (affects tag insertion)
//   - 0x04: COMPRESSED - MsgBytes is zstd compressed
func (c *Conn) handleBroadcast(data []byte) {
	if len(data) < 2 {
		logger.Warn("BROADCAST too short", "server_id", c.backend.ServerID, "length", len(data))
		return
	}

	flags := data[1]
	payload := data[2:]

	// Decode the broadcast payload
	broadcast, err := DecodeBroadcastPayload(payload)
	if err != nil {
		logger.Error("Failed to decode BROADCAST payload", "server_id", c.backend.ServerID, "error", err)
		return
	}

	// Get the message bytes
	msgBytes := broadcast.Message

	// --- Stage 3: Decompression (future) ---
	// When COMPRESSED flag is set, decompress msgBytes before fan-out
	if flags&BroadcastFlagCompressed != 0 {
		decompressed, err := zstdDecoder.DecodeAll(msgBytes, nil)
		if err != nil {
			logger.Error("Failed to decompress BROADCAST message", "server_id", c.backend.ServerID, "error", err)
			return
		}
		msgBytes = decompressed
	}

	reliable := flags&BroadcastFlagReliable != 0
	isFirebase := flags&BroadcastFlagFirebase != 0

	// Fan out to each client
	for _, client := range broadcast.Clients {
		msg := msgBytes

		// Tag insertion: when Tag != 0, insert tag into the JSON message
		if client.Tag != 0 {
			if isFirebase {
				msg = insertTagFirebase(msgBytes, client.Tag)
			} else {
				msg = insertTagLark(msgBytes, client.Tag)
			}
		}

		c.deliverToClient(client.ID, msg, reliable)
	}
}

// dispatchClientMessage handles a single client-bound message (DATA or CLOSE).
// Called inline from readLoop for maximum throughput.
func (c *Conn) dispatchClientMessage(msgType byte, clientID uint32, payload []byte) {
	if c.backend.pool.clients == nil {
		return
	}

	client := c.backend.pool.clients.GetClient(clientID)
	if client == nil {
		return
	}

	switch msgType {
	case MsgTypeSendData:
		// Decode the data payload to extract flags (reliable/unreliable/compressed)
		dataPayload, err := DecodeDataPayload(payload)
		if err != nil {
			logger.Warn("Invalid data payload", "server_id", c.backend.ServerID, "error", err)
			return
		}

		data := dataPayload.Data

		// Decompress if needed
		if dataPayload.Flags&FlagCompressed != 0 {
			decompressed, err := zstdDecoder.DecodeAll(data, nil)
			if err != nil {
				logger.Error("Failed to decompress DATA message", "server_id", c.backend.ServerID, "client_id", clientID, "error", err)
				return
			}
			data = decompressed
		}

		reliable := dataPayload.Flags&FlagReliable != 0

		// Non-blocking deliver to client outbox
		// If false, client is too slow - disconnect them
		if !client.Deliver(data, reliable) {
			logger.Debug("Client outbox full, disconnecting", "server_id", c.backend.ServerID, "client_id", clientID)
			client.Close()
		}

	case MsgTypeClose:
		client.Close()
	}
}

// deliverToClient delivers a message directly to a client (used by COMPRESSED_MULTI).
// The data is already decoded - no flags byte prefix.
func (c *Conn) deliverToClient(clientID uint32, data []byte, reliable bool) {
	if c.backend.pool.clients == nil {
		return
	}

	client := c.backend.pool.clients.GetClient(clientID)
	if client == nil {
		return
	}

	if !client.Deliver(data, reliable) {
		logger.Debug("Client outbox full, disconnecting", "server_id", c.backend.ServerID, "client_id", clientID)
		client.Close()
	}
}

// WriteMessage writes a client message to the connection's buffer
func (c *Conn) WriteMessage(msg *Message) error {
	if c.closed.Load() {
		return ErrPoolClosed
	}
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	return WriteMessage(c.writer, msg)
}

// WriteControlMessage writes a control message (no ClientID) to the connection
func (c *Conn) WriteControlMessage(msgType byte, payload []byte) error {
	if c.closed.Load() {
		return ErrPoolClosed
	}
	c.writerMu.Lock()
	defer c.writerMu.Unlock()

	if err := WriteControlMessage(c.writer, msgType, payload); err != nil {
		return err
	}
	return c.writer.Flush()
}

// Flush flushes the write buffer
func (c *Conn) Flush() error {
	if c.closed.Load() {
		return nil
	}
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	return c.writer.Flush()
}

// Close closes the connection
func (c *Conn) Close() {
	if c.closed.Swap(true) {
		return // Already closed
	}
	c.conn.Close()
}

// IsClosed returns true if the connection is closed
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

// LocalAddr returns the local address
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote address
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
