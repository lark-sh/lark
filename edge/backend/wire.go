// Wire Protocol Implementation
//
// This file defines the binary wire protocol used between proxy and backend servers.
// The protocol is length-prefixed and designed for efficient multiplexing of many
// client connections over a single TCP connection.
//
// # Message Format
//
// All messages follow this structure:
//
//	┌──────────────┬──────────┬────────────┬─────────────────┐
//	│ Length (4B)  │ Type(1B) │ ClientID   │ Payload         │
//	│ big-endian   │          │ (4B)       │ (variable)      │
//	└──────────────┴──────────┴────────────┴─────────────────┘
//
// Length: Total message size (excluding the 4-byte length field itself)
// Type: One of the MsgType* constants defined below
// ClientID: 32-bit client identifier (0 for control messages)
// Payload: Type-specific data
//
// # Proxy → Backend Messages
//
//	CONNECT (0x01): New client connection
//	  Payload: protocol(1B) + projectLen(1B) + project + dbLen(1B) + database + authJSON
//
//	DATA (0x02): Client message data
//	  Payload: raw message bytes
//
//	DISCONNECT (0x03): Client disconnected
//	  Payload: reason(1B)
//
//	HELLO (0x04): Connection handshake
//	  Payload: version(4B, big-endian)
//
//	AUTH_CHANGED (0x05): Client auth updated (late authentication)
//	  Payload: authJSON
//
//	HEARTBEAT_ACK (0x06): Response to server heartbeat
//	  Payload: serverID + timestamp
//
//	CONFIG_PUSH (0x07): Push project configuration to server
//	  Payload: configJSON
//
//	EVICT_DATABASE (0x08): Request database eviction
//	  Payload: projectLen(1B) + project + dbLen(1B) + database + flags(1B)
//	  Flags: bit 0 = PURGE_DATA (delete on-disk data for persistent dbs)
//
//	SHUTDOWN (0x09): Graceful shutdown request
//	  Payload: (empty)
//
// # Backend → Proxy Messages
//
//	SEND_DATA (0x01): Send data to client
//	  Payload: reliable(1B) + data
//
//	CLOSE (0x02): Close client connection
//	  Payload: (empty)
//
//	HELLO_ACK (0x03): Handshake response
//	  Payload: serverID + nrCores(4B)
//
//	HEARTBEAT (0x04): Server health metrics
//	  Payload: load(2B) + clients(4B) + memMB(4B) per core
//
//	DATABASE_LOADED (0x05): Database now active
//	  Payload: projectLen(1B) + project + dbLen(1B) + database
//
//	DATABASE_UNLOADED (0x06): Database evicted
//	  Payload: reason(1B) + projectLen(1B) + project + dbLen(1B) + database
//
//	CONFIG_REQUEST (0x07): Request project configuration
//	  Payload: projectID
//
// # Buffer Management
//
// Encoding functions allocate new buffers on each call. For hot paths (DATA messages),
// consider using sync.Pool to reduce allocations. The headerPool and lenBufPool are
// available but underutilized.
//
// # Thread Safety
//
// Encoding functions are stateless and thread-safe. Decoding functions are also
// thread-safe but the returned payloads share memory with the input buffer.
package backend

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// Message types from Proxy to Server
const (
	MsgTypeConnect       byte = 0x01 // New client connected
	MsgTypeData          byte = 0x02 // Client sent data
	MsgTypeDisconnect    byte = 0x03 // Client disconnected
	MsgTypeHello         byte = 0x04 // Connection handshake
	MsgTypeAuthChanged   byte = 0x05 // Client auth status changed (late auth)
	MsgTypeHeartbeatAck  byte = 0x06 // Heartbeat acknowledgment
	MsgTypeConfigPush    byte = 0x07 // Push project configuration
	MsgTypeEvictDatabase byte = 0x08 // Force database eviction
	MsgTypeShutdown      byte = 0x09 // Graceful shutdown request
)

// Message types from Server to Proxy
const (
	MsgTypeSendData         byte = 0x01 // Send data to client
	MsgTypeClose            byte = 0x02 // Close client connection
	MsgTypeHelloAck         byte = 0x03 // Connection handshake response
	MsgTypeHeartbeat        byte = 0x04 // Health metrics report
	MsgTypeDatabaseLoaded   byte = 0x05 // Database now active on core
	MsgTypeDatabaseUnloaded byte = 0x06 // Database evicted from core
	MsgTypeConfigRequest    byte = 0x07 // Request project configuration
	MsgTypeCompressedMulti  byte = 0x0A // Compressed batch of messages for multiple clients
	MsgTypeBroadcast        byte = 0x0B // Broadcast same message to multiple clients
)

// Broadcast flags (for BROADCAST messages)
const (
	BroadcastFlagReliable   byte = 0x01 // Use reliable delivery
	BroadcastFlagFirebase   byte = 0x02 // Message is Firebase format (affects tag insertion)
	BroadcastFlagCompressed byte = 0x04 // MsgBytes is zstd compressed
)

// Protocol types
const (
	ProtocolWebSocket    byte = 0x00
	ProtocolWebTransport byte = 0x01
	ProtocolREST         byte = 0x02 // REST API (256MB response limit)
)

// Disconnect reasons
const (
	DisconnectClean   byte = 0x00
	DisconnectError   byte = 0x01
	DisconnectTimeout byte = 0x02
)

// Database unload reasons
const (
	UnloadReasonIdle           byte = 0x00 // Idle timeout
	UnloadReasonMemoryPressure byte = 0x01 // Memory pressure
	UnloadReasonEvicted        byte = 0x02 // Explicit eviction
	UnloadReasonShutdown       byte = 0x03 // Server shutdown
)

// Message flags (for backend -> proxy DATA messages)
const (
	FlagReliable   byte = 0x01 // Send reliably (for WebTransport, use stream; for WebSocket, always reliable)
	FlagUnreliable byte = 0x00 // Send unreliably (for WebTransport, use datagram)
	FlagCompressed byte = 0x02 // Payload is ZSTD compressed (DATA and COMPRESSED_MULTI messages)
)

var (
	ErrMessageTooLarge = errors.New("message too large")
	ErrInvalidMessage  = errors.New("invalid message")
)

// Buffer pools to reduce allocations
var (
	headerPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 9) // 4 (length) + 1 (type) + 4 (clientID)
			return &buf
		},
	}
	lenBufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 4)
			return &buf
		},
	}
)

const MaxMessageSize = 256 * 1024 * 1024 // 256MB max message size (matches max read size)

// Message represents a wire protocol message
type Message struct {
	Type     byte
	ClientID uint32
	Payload  []byte
}

// ConnectPayload is the payload for CONNECT messages
type ConnectPayload struct {
	Protocol   byte   // ProtocolWebSocket or ProtocolWebTransport
	ProjectID  string
	DatabaseID string
	Metadata   []byte // Optional JSON metadata (IP, headers, etc.)
	Auth       []byte // JSON-encoded AuthPayload (auth validated by proxy)
}

// ErrIDTooLong is returned when a project or database ID exceeds 255 bytes
var ErrIDTooLong = errors.New("project or database ID exceeds 255 bytes")

// EncodeConnectPayload encodes a connect payload.
// Returns ErrIDTooLong if project or database ID exceeds 255 bytes.
func EncodeConnectPayload(p *ConnectPayload) ([]byte, error) {
	projectLen := len(p.ProjectID)
	databaseLen := len(p.DatabaseID)

	if projectLen > 255 || databaseLen > 255 {
		return nil, ErrIDTooLong
	}

	metadataLen := len(p.Metadata)
	authLen := len(p.Auth)

	buf := make([]byte, 1+1+projectLen+1+databaseLen+2+metadataLen+2+authLen)
	offset := 0

	buf[offset] = p.Protocol
	offset++

	buf[offset] = byte(projectLen)
	offset++
	copy(buf[offset:], p.ProjectID)
	offset += projectLen

	buf[offset] = byte(databaseLen)
	offset++
	copy(buf[offset:], p.DatabaseID)
	offset += databaseLen

	binary.BigEndian.PutUint16(buf[offset:], uint16(metadataLen))
	offset += 2
	if metadataLen > 0 {
		copy(buf[offset:], p.Metadata)
		offset += metadataLen
	}

	binary.BigEndian.PutUint16(buf[offset:], uint16(authLen))
	offset += 2
	if authLen > 0 {
		copy(buf[offset:], p.Auth)
	}

	return buf, nil
}

// DecodeConnectPayload decodes a connect payload
func DecodeConnectPayload(data []byte) (*ConnectPayload, error) {
	if len(data) < 4 {
		return nil, ErrInvalidMessage
	}

	p := &ConnectPayload{}
	offset := 0

	p.Protocol = data[offset]
	offset++

	projectLen := int(data[offset])
	offset++
	if offset+projectLen > len(data) {
		return nil, ErrInvalidMessage
	}
	p.ProjectID = string(data[offset : offset+projectLen])
	offset += projectLen

	databaseLen := int(data[offset])
	offset++
	if offset+databaseLen > len(data) {
		return nil, ErrInvalidMessage
	}
	p.DatabaseID = string(data[offset : offset+databaseLen])
	offset += databaseLen

	if offset+2 > len(data) {
		return nil, ErrInvalidMessage
	}
	metadataLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if metadataLen > 0 {
		if offset+metadataLen > len(data) {
			return nil, ErrInvalidMessage
		}
		p.Metadata = data[offset : offset+metadataLen]
		offset += metadataLen
	}

	// Auth field (added in v2 of protocol)
	// Check if there's more data for backwards compatibility
	if offset+2 <= len(data) {
		authLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if authLen > 0 {
			if offset+authLen > len(data) {
				return nil, ErrInvalidMessage
			}
			p.Auth = data[offset : offset+authLen]
		}
	}

	return p, nil
}

// EncodeAuthPayload encodes an AuthPayload to JSON bytes
func EncodeAuthPayload(auth *AuthPayload) ([]byte, error) {
	return json.Marshal(auth)
}

// DecodeAuthPayload decodes JSON bytes to an AuthPayload
func DecodeAuthPayload(data []byte) (*AuthPayload, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var auth AuthPayload
	if err := json.Unmarshal(data, &auth); err != nil {
		return nil, err
	}
	return &auth, nil
}

// DataPayload is the payload for backend -> proxy DATA messages
type DataPayload struct {
	Flags byte   // FlagReliable or FlagUnreliable
	Data  []byte
}

// AuthPayload contains authentication information validated by the proxy.
// Sent as part of CONNECT message and AUTH_CHANGED messages.
type AuthPayload struct {
	UID         string         `json:"uid"`                // User ID (empty for anonymous)
	Provider    string         `json:"provider"`           // Auth provider: "anonymous", "google", "custom", etc.
	Claims      map[string]any `json:"claims,omitempty"`   // Custom claims (auth.token in rules)
	IsTrueAdmin bool           `json:"is_admin,omitempty"` // True if coordinator admin token
}

// EncodeDataPayload encodes a data payload for backend -> proxy
func EncodeDataPayload(flags byte, data []byte) []byte {
	buf := make([]byte, 1+len(data))
	buf[0] = flags
	copy(buf[1:], data)
	return buf
}

// DecodeDataPayload decodes a data payload from backend -> proxy
func DecodeDataPayload(data []byte) (*DataPayload, error) {
	if len(data) < 1 {
		return nil, ErrInvalidMessage
	}
	return &DataPayload{
		Flags: data[0],
		Data:  data[1:],
	}, nil
}

// WriteMessage writes a message to a writer
func WriteMessage(w io.Writer, msg *Message) error {
	// Header: length (4) + type (1) + clientID (4) = 9 bytes
	payloadLen := len(msg.Payload)
	if payloadLen > MaxMessageSize {
		return ErrMessageTooLarge
	}

	totalLen := 1 + 4 + payloadLen // type + clientID + payload

	// Get header buffer from pool
	headerPtr := headerPool.Get().(*[]byte)
	header := *headerPtr
	defer headerPool.Put(headerPtr)

	binary.BigEndian.PutUint32(header[0:4], uint32(totalLen))
	header[4] = msg.Type
	binary.BigEndian.PutUint32(header[5:9], msg.ClientID)

	if _, err := w.Write(header); err != nil {
		return err
	}

	if payloadLen > 0 {
		if _, err := w.Write(msg.Payload); err != nil {
			return err
		}
	}

	return nil
}

// ReadMessage reads a message from a reader
func ReadMessage(r io.Reader) (*Message, error) {
	// Get length buffer from pool
	lenBufPtr := lenBufPool.Get().(*[]byte)
	lenBuf := *lenBufPtr
	defer lenBufPool.Put(lenBufPtr)

	// Read length
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf)

	if totalLen < 5 { // minimum: type (1) + clientID (4)
		return nil, ErrInvalidMessage
	}
	if totalLen > MaxMessageSize+5 {
		return nil, ErrMessageTooLarge
	}

	// Read rest of message
	data := make([]byte, totalLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	msg := &Message{
		Type:     data[0],
		ClientID: binary.BigEndian.Uint32(data[1:5]),
	}

	if len(data) > 5 {
		msg.Payload = data[5:]
	}

	return msg, nil
}

// ProxyVersion is the current proxy wire protocol version
const ProxyVersion uint16 = 1

// HelloPayload is the payload for HELLO messages (proxy -> server)
type HelloPayload struct {
	ProxyVersion uint16
}

// HelloAckPayload is the payload for HELLO_ACK messages (server -> proxy)
type HelloAckPayload struct {
	CoreID        uint8
	NrCores       uint8
	ServerVersion uint16
}

// WriteHello writes a HELLO message to establish a connection
// Format: [Length:4][Type:1][ProxyVersion:2][Reserved:5]
func WriteHello(w io.Writer, proxyVersion uint16) error {
	// Total message content: type(1) + proxyVersion(2) + reserved(5) = 8 bytes
	buf := make([]byte, 12) // 4 (length) + 8 (content)

	binary.BigEndian.PutUint32(buf[0:4], 8) // length = 8
	buf[4] = MsgTypeHello
	binary.BigEndian.PutUint16(buf[5:7], proxyVersion)
	// buf[7:12] are reserved (zero)

	_, err := w.Write(buf)
	return err
}

// ReadHelloAck reads a HELLO_ACK message from the server
// Format: [Length:4][Type:1][CoreID:1][NrCores:1][ServerVersion:2][Reserved:4]
func ReadHelloAck(r io.Reader) (*HelloAckPayload, error) {
	// Read length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf)

	if totalLen != 9 { // type(1) + coreID(1) + nrCores(1) + serverVersion(2) + reserved(4)
		return nil, ErrInvalidMessage
	}

	// Read content
	data := make([]byte, totalLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	if data[0] != MsgTypeHelloAck {
		return nil, ErrInvalidMessage
	}

	return &HelloAckPayload{
		CoreID:        data[1],
		NrCores:       data[2],
		ServerVersion: binary.BigEndian.Uint16(data[3:5]),
	}, nil
}

// CoreForDatabase computes which core owns a database using xxhash64
// This must match the server's sharding function exactly
func CoreForDatabase(databaseID string, nrCores int) int {
	hash := xxhash.Sum64String(databaseID)
	return int(hash % uint64(nrCores))
}

// =============================================================================
// Coordinator Protocol Messages
// =============================================================================

// HeartbeatPayload is the payload for HEARTBEAT messages (server -> proxy)
// Sent every 10 seconds on all connections
type HeartbeatPayload struct {
	Load    uint16 // CPU load 0-10000 (0.00%-100.00%)
	Clients uint32 // Active client count on this core
	MemMB   uint32 // Memory used by this core in MB
}

// EncodeHeartbeat encodes a HEARTBEAT payload
// Format: [Load:2][Clients:4][MemMB:4][Reserved:6]
func EncodeHeartbeat(hb *HeartbeatPayload) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint16(buf[0:2], hb.Load)
	binary.BigEndian.PutUint32(buf[2:6], hb.Clients)
	binary.BigEndian.PutUint32(buf[6:10], hb.MemMB)
	// buf[10:16] reserved
	return buf
}

// DecodeHeartbeat decodes a HEARTBEAT payload
func DecodeHeartbeat(data []byte) (*HeartbeatPayload, error) {
	if len(data) < 10 {
		return nil, ErrInvalidMessage
	}
	return &HeartbeatPayload{
		Load:    binary.BigEndian.Uint16(data[0:2]),
		Clients: binary.BigEndian.Uint32(data[2:6]),
		MemMB:   binary.BigEndian.Uint32(data[6:10]),
	}, nil
}

// HeartbeatAckPayload is the payload for HEARTBEAT_ACK messages (proxy -> server)
type HeartbeatAckPayload struct {
	ServerTime uint64 // Unix milliseconds (for clock sync)
}

// EncodeHeartbeatAck encodes a HEARTBEAT_ACK payload
// Format: [ServerTime:8][Reserved:4]
func EncodeHeartbeatAck(serverTime uint64) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], serverTime)
	// buf[8:12] reserved
	return buf
}

// DecodeHeartbeatAck decodes a HEARTBEAT_ACK payload
func DecodeHeartbeatAck(data []byte) (*HeartbeatAckPayload, error) {
	if len(data) < 8 {
		return nil, ErrInvalidMessage
	}
	return &HeartbeatAckPayload{
		ServerTime: binary.BigEndian.Uint64(data[0:8]),
	}, nil
}

// DatabaseLoadedPayload is the payload for DATABASE_LOADED messages (server -> proxy)
type DatabaseLoadedPayload struct {
	ProjectID  string
	DatabaseID string
}

// EncodeDatabaseLoaded encodes a DATABASE_LOADED payload
// Format: [ProjLen:1][ProjectID:var][DBLen:1][DatabaseID:var]
func EncodeDatabaseLoaded(projectID, databaseID string) []byte {
	projLen := len(projectID)
	dbLen := len(databaseID)
	buf := make([]byte, 1+projLen+1+dbLen)

	buf[0] = byte(projLen)
	copy(buf[1:1+projLen], projectID)
	buf[1+projLen] = byte(dbLen)
	copy(buf[2+projLen:], databaseID)

	return buf
}

// DecodeDatabaseLoaded decodes a DATABASE_LOADED payload
func DecodeDatabaseLoaded(data []byte) (*DatabaseLoadedPayload, error) {
	if len(data) < 2 {
		return nil, ErrInvalidMessage
	}

	offset := 0
	projLen := int(data[offset])
	offset++

	if offset+projLen >= len(data) {
		return nil, ErrInvalidMessage
	}
	projectID := string(data[offset : offset+projLen])
	offset += projLen

	dbLen := int(data[offset])
	offset++

	if offset+dbLen > len(data) {
		return nil, ErrInvalidMessage
	}
	databaseID := string(data[offset : offset+dbLen])

	return &DatabaseLoadedPayload{
		ProjectID:  projectID,
		DatabaseID: databaseID,
	}, nil
}

// DatabaseUnloadedPayload is the payload for DATABASE_UNLOADED messages (server -> proxy)
type DatabaseUnloadedPayload struct {
	ProjectID  string
	DatabaseID string
	Reason     byte
	Ephemeral  bool
}

// EncodeDatabaseUnloaded encodes a DATABASE_UNLOADED payload
// Format: [ProjLen:1][ProjectID:var][DBLen:1][DatabaseID:var][Reason:1][Ephemeral:1]
func EncodeDatabaseUnloaded(projectID, databaseID string, reason byte, ephemeral bool) []byte {
	projLen := len(projectID)
	dbLen := len(databaseID)
	buf := make([]byte, 1+projLen+1+dbLen+1+1)

	buf[0] = byte(projLen)
	copy(buf[1:1+projLen], projectID)
	buf[1+projLen] = byte(dbLen)
	copy(buf[2+projLen:2+projLen+dbLen], databaseID)
	buf[2+projLen+dbLen] = reason
	if ephemeral {
		buf[3+projLen+dbLen] = 1
	} else {
		buf[3+projLen+dbLen] = 0
	}

	return buf
}

// DecodeDatabaseUnloaded decodes a DATABASE_UNLOADED payload
func DecodeDatabaseUnloaded(data []byte) (*DatabaseUnloadedPayload, error) {
	if len(data) < 4 {
		return nil, ErrInvalidMessage
	}

	offset := 0
	projLen := int(data[offset])
	offset++

	if offset+projLen >= len(data) {
		return nil, ErrInvalidMessage
	}
	projectID := string(data[offset : offset+projLen])
	offset += projLen

	dbLen := int(data[offset])
	offset++

	if offset+dbLen >= len(data) {
		return nil, ErrInvalidMessage
	}
	databaseID := string(data[offset : offset+dbLen])
	offset += dbLen

	if offset >= len(data) {
		return nil, ErrInvalidMessage
	}
	reason := data[offset]
	offset++

	ephemeral := false
	if offset < len(data) {
		ephemeral = data[offset] != 0
	}

	return &DatabaseUnloadedPayload{
		ProjectID:  projectID,
		DatabaseID: databaseID,
		Reason:     reason,
		Ephemeral:  ephemeral,
	}, nil
}

// ConfigRequestPayload is the payload for CONFIG_REQUEST messages (server -> proxy)
type ConfigRequestPayload struct {
	ProjectID string
}

// EncodeConfigRequest encodes a CONFIG_REQUEST payload
// Format: [ProjLen:1][ProjectID:var]
func EncodeConfigRequest(projectID string) []byte {
	projLen := len(projectID)
	buf := make([]byte, 1+projLen)
	buf[0] = byte(projLen)
	copy(buf[1:], projectID)
	return buf
}

// DecodeConfigRequest decodes a CONFIG_REQUEST payload
func DecodeConfigRequest(data []byte) (*ConfigRequestPayload, error) {
	if len(data) < 1 {
		return nil, ErrInvalidMessage
	}

	projLen := int(data[0])
	if 1+projLen > len(data) {
		return nil, ErrInvalidMessage
	}

	return &ConfigRequestPayload{
		ProjectID: string(data[1 : 1+projLen]),
	}, nil
}

// ProjectConfig is the configuration pushed to servers.
//
// ConfigVersion is a monotonic counter bumped on every admin write to the project.
// Backends cache the version alongside the config and ignore CONFIG_PUSH messages
// where the incoming version is <= the cached version. This provides idempotent
// fan-out: multiple proxies can push the same version in response to a NOTIFY and
// the backend applies only once.
type ProjectConfig struct {
	Rules             string         `json:"rules"`
	SecretKey         string         `json:"secret_key"`
	AdminSecretKey    string         `json:"admin_secret_key"`
	FirebaseProjectID string         `json:"firebase_project_id,omitempty"`
	Ephemeral         bool           `json:"ephemeral"`
	ConfigVersion     int64          `json:"config_version"`
	Settings          map[string]any `json:"settings,omitempty"`
}

// ConfigPushPayload is the payload for CONFIG_PUSH messages (proxy -> server)
type ConfigPushPayload struct {
	ProjectID string
	Config    *ProjectConfig
}

// EncodeConfigPush encodes a CONFIG_PUSH payload
// Format: [ProjLen:1][ProjectID:var][ConfigLen:4][ConfigJSON:var]
func EncodeConfigPush(projectID string, config *ProjectConfig) ([]byte, error) {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}

	projLen := len(projectID)
	configLen := len(configJSON)
	buf := make([]byte, 1+projLen+4+configLen)

	buf[0] = byte(projLen)
	copy(buf[1:1+projLen], projectID)
	binary.BigEndian.PutUint32(buf[1+projLen:5+projLen], uint32(configLen))
	copy(buf[5+projLen:], configJSON)

	return buf, nil
}

// DecodeConfigPush decodes a CONFIG_PUSH payload
func DecodeConfigPush(data []byte) (*ConfigPushPayload, error) {
	if len(data) < 5 {
		return nil, ErrInvalidMessage
	}

	offset := 0
	projLen := int(data[offset])
	offset++

	if offset+projLen+4 > len(data) {
		return nil, ErrInvalidMessage
	}
	projectID := string(data[offset : offset+projLen])
	offset += projLen

	configLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+configLen > len(data) {
		return nil, ErrInvalidMessage
	}

	var config ProjectConfig
	if err := json.Unmarshal(data[offset:offset+configLen], &config); err != nil {
		return nil, err
	}

	return &ConfigPushPayload{
		ProjectID: projectID,
		Config:    &config,
	}, nil
}

// EvictDatabasePayload is the payload for EVICT_DATABASE messages (proxy -> server)
type EvictDatabasePayload struct {
	ProjectID  string
	DatabaseID string
	Purge      bool // Delete on-disk data for persistent databases (admin-initiated delete)
}

// EvictDatabase flag bits
const (
	EvictFlagPurgeData byte = 0x01 // Delete on-disk data in addition to unloading
)

// EncodeEvictDatabase encodes an EVICT_DATABASE payload.
// Format: [ProjLen:1][ProjectID:var][DBLen:1][DatabaseID:var][Flags:1]
//
// The trailing flags byte distinguishes routine eviction (unload from memory) from
// admin-initiated delete (unload + purge on-disk data). Purging is only meaningful
// for persistent databases; ephemeral ones have no on-disk state.
func EncodeEvictDatabase(projectID, databaseID string, purge bool) []byte {
	base := EncodeDatabaseLoaded(projectID, databaseID)
	buf := make([]byte, len(base)+1)
	copy(buf, base)
	if purge {
		buf[len(base)] = EvictFlagPurgeData
	}
	return buf
}

// DecodeEvictDatabase decodes an EVICT_DATABASE payload.
// Accepts payloads without a trailing flags byte for forward/backward compatibility
// (defaults to purge=false).
func DecodeEvictDatabase(data []byte) (*EvictDatabasePayload, error) {
	loaded, err := DecodeDatabaseLoaded(data)
	if err != nil {
		return nil, err
	}

	// Header: [ProjLen:1][ProjectID:var][DBLen:1][DatabaseID:var]
	headerLen := 1 + len(loaded.ProjectID) + 1 + len(loaded.DatabaseID)
	var purge bool
	if len(data) > headerLen {
		purge = data[headerLen]&EvictFlagPurgeData != 0
	}

	return &EvictDatabasePayload{
		ProjectID:  loaded.ProjectID,
		DatabaseID: loaded.DatabaseID,
		Purge:      purge,
	}, nil
}

// ShutdownPayload is the payload for SHUTDOWN messages (proxy -> server)
type ShutdownPayload struct {
	GracePeriodSec uint32
}

// EncodeShutdown encodes a SHUTDOWN payload
// Format: [GracePeriodSec:4][Reserved:4]
func EncodeShutdown(gracePeriodSec uint32) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], gracePeriodSec)
	// buf[4:8] reserved
	return buf
}

// DecodeShutdown decodes a SHUTDOWN payload
func DecodeShutdown(data []byte) (*ShutdownPayload, error) {
	if len(data) < 4 {
		return nil, ErrInvalidMessage
	}
	return &ShutdownPayload{
		GracePeriodSec: binary.BigEndian.Uint32(data[0:4]),
	}, nil
}

// WriteControlMessage writes a control message (no ClientID) to a writer
// Used for HEARTBEAT_ACK, CONFIG_PUSH, EVICT_DATABASE, SHUTDOWN
// Format: [Length:4][Type:1][Payload:var]
func WriteControlMessage(w io.Writer, msgType byte, payload []byte) error {
	payloadLen := len(payload)
	if payloadLen > MaxMessageSize {
		return ErrMessageTooLarge
	}

	totalLen := 1 + payloadLen // type + payload

	// Write length
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(totalLen))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}

	// Write type
	if _, err := w.Write([]byte{msgType}); err != nil {
		return err
	}

	// Write payload
	if payloadLen > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	return nil
}

// ControlMessage represents a control message (no ClientID)
type ControlMessage struct {
	Type    byte
	Payload []byte
}

// CompressedMultiMessage represents a single message within a COMPRESSED_MULTI batch
type CompressedMultiMessage struct {
	ClientID uint32
	Data     []byte
}

// BroadcastClient represents a client in a BROADCAST message
type BroadcastClient struct {
	ID  uint32
	Tag int32
}

// BroadcastPayload represents the decoded payload of a BROADCAST message
type BroadcastPayload struct {
	Clients []BroadcastClient
	Message []byte
}

// DecodeBroadcastPayload decodes the payload of a BROADCAST message.
// Format: [ClientCount:4][[ClientID:4][Tag:4]]...[MsgLen:4][MsgBytes:var]
func DecodeBroadcastPayload(data []byte) (*BroadcastPayload, error) {
	if len(data) < 4 {
		return nil, ErrInvalidMessage
	}

	clientCount := binary.BigEndian.Uint32(data[0:4])

	// Sanity check - cap at 256k clients
	if clientCount > 262144 {
		return nil, ErrInvalidMessage
	}

	// Calculate expected size for client list
	clientListSize := int(clientCount) * 8 // 4 bytes ID + 4 bytes Tag per client
	if len(data) < 4+clientListSize+4 {    // count + clients + msgLen
		return nil, ErrInvalidMessage
	}

	clients := make([]BroadcastClient, clientCount)
	offset := 4

	for i := uint32(0); i < clientCount; i++ {
		clients[i].ID = binary.BigEndian.Uint32(data[offset:])
		clients[i].Tag = int32(binary.BigEndian.Uint32(data[offset+4:]))
		offset += 8
	}

	// Parse message
	if offset+4 > len(data) {
		return nil, ErrInvalidMessage
	}
	msgLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	if offset+int(msgLen) > len(data) {
		return nil, ErrInvalidMessage
	}
	message := data[offset : offset+int(msgLen)]

	return &BroadcastPayload{
		Clients: clients,
		Message: message,
	}, nil
}

// DecodeCompressedMultiPayload decodes the inner payload of a COMPRESSED_MULTI message.
// The input should already be decompressed.
// Format: [MessageCount:4][[ClientID:4][MessageLength:4][MessageBytes:var]]...
func DecodeCompressedMultiPayload(data []byte) ([]CompressedMultiMessage, error) {
	if len(data) < 4 {
		return nil, ErrInvalidMessage
	}

	count := binary.BigEndian.Uint32(data[0:4])
	offset := 4

	// Sanity check - prevent allocating huge slice on malformed data
	if count > 1000000 {
		return nil, ErrInvalidMessage
	}

	messages := make([]CompressedMultiMessage, 0, count)

	for i := uint32(0); i < count; i++ {
		if offset+8 > len(data) {
			return nil, ErrInvalidMessage
		}

		clientID := binary.BigEndian.Uint32(data[offset:])
		msgLen := binary.BigEndian.Uint32(data[offset+4:])
		offset += 8

		if offset+int(msgLen) > len(data) {
			return nil, ErrInvalidMessage
		}

		messages = append(messages, CompressedMultiMessage{
			ClientID: clientID,
			Data:     data[offset : offset+int(msgLen)],
		})
		offset += int(msgLen)
	}

	return messages, nil
}

// ReadControlMessage reads a control message (no ClientID) from a reader
// Used for messages like HEARTBEAT, DATABASE_LOADED, CONFIG_REQUEST
func ReadControlMessage(r io.Reader) (*ControlMessage, error) {
	// Read length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf)

	if totalLen < 1 {
		return nil, ErrInvalidMessage
	}
	if totalLen > MaxMessageSize+1 {
		return nil, ErrMessageTooLarge
	}

	// Read content
	data := make([]byte, totalLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	msg := &ControlMessage{
		Type: data[0],
	}

	if len(data) > 1 {
		msg.Payload = data[1:]
	}

	return msg, nil
}
