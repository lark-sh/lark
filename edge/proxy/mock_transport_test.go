package proxy

import (
	"sync"

	"github.com/lark-sh/lark/edge/backend"
)

// MockTransport implements ClientTransport for testing
type MockTransport struct {
	mu            sync.Mutex
	sentMessages  []sentMessage
	closed        bool
	transportType byte
}

type sentMessage struct {
	data     []byte
	reliable bool
}

// NewMockTransport creates a new mock transport
func NewMockTransport(transportType byte) *MockTransport {
	return &MockTransport{
		transportType: transportType,
	}
}

// Send implements ClientTransport
func (m *MockTransport) Send(data []byte, reliable bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	// Copy data to avoid aliasing issues
	copied := make([]byte, len(data))
	copy(copied, data)

	m.sentMessages = append(m.sentMessages, sentMessage{
		data:     copied,
		reliable: reliable,
	})
	return nil
}

// Close implements ClientTransport
func (m *MockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// TransportType implements ClientTransport
func (m *MockTransport) TransportType() byte {
	return m.transportType
}

// SentMessages returns all sent messages (for testing)
func (m *MockTransport) SentMessages() []sentMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sentMessages
}

// LastMessage returns the last sent message (for testing)
func (m *MockTransport) LastMessage() ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sentMessages) == 0 {
		return nil, false
	}
	last := m.sentMessages[len(m.sentMessages)-1]
	return last.data, true
}

// MessageCount returns the number of sent messages (for testing)
func (m *MockTransport) MessageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sentMessages)
}

// IsClosed returns whether the transport was closed (for testing)
func (m *MockTransport) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Reset clears all sent messages (for testing)
func (m *MockTransport) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = nil
	m.closed = false
}

// MockBackend implements a mock backend for testing
type MockBackend struct {
	mu       sync.Mutex
	messages []*backend.Message
}

// NewMockBackend creates a new mock backend
func NewMockBackend() *MockBackend {
	return &MockBackend{}
}

// SendMessage records a message (for testing)
func (m *MockBackend) SendMessage(msg *backend.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return nil
}

// Messages returns all received messages (for testing)
func (m *MockBackend) Messages() []*backend.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messages
}

// LastMessage returns the last received message (for testing)
func (m *MockBackend) LastMessage() *backend.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.messages) == 0 {
		return nil
	}
	return m.messages[len(m.messages)-1]
}

// MessageCount returns the number of received messages (for testing)
func (m *MockBackend) MessageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

// Reset clears all messages (for testing)
func (m *MockBackend) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = nil
}
