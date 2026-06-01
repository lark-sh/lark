// Package logger provides structured JSON logging for lark-edge.
//
// Output is line-delimited JSON to stdout, suitable for ingestion by any
// log collector that reads from stdout or journald. Format:
//
//	{"type":"log","level":"info","ts":1706011200,"message":"Client connected","client_id":"abc123"}
//
// Usage:
//
//	logger.Info("Client connected", "client_id", clientID, "project", projectID)
//	logger.Error("Failed to route", "error", err, "client_id", clientID)
//	logger.Warn("Rate limited", "client_id", clientID, "hits", 50)
package logger

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Level represents log severity
type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

var (
	// mu protects writes to stdout
	mu sync.Mutex

	// encoder writes JSON to stdout
	encoder = json.NewEncoder(os.Stdout)

	// minLevel filters logs below this level
	minLevel = LevelInfo

	// serverName identifies this server in logs
	serverName = "proxy"
)

// SetLevel sets the minimum log level
func SetLevel(level Level) {
	minLevel = level
}

// SetServerName sets the server name included in all logs
func SetServerName(name string) {
	serverName = name
}

// shouldLog returns true if the level should be logged
func shouldLog(level Level) bool {
	switch minLevel {
	case LevelDebug:
		return true
	case LevelInfo:
		return level != LevelDebug
	case LevelWarn:
		return level == LevelWarn || level == LevelError
	case LevelError:
		return level == LevelError
	}
	return true
}

// log writes a structured log entry
func log(level Level, message string, kvs ...interface{}) {
	if !shouldLog(level) {
		return
	}

	entry := map[string]interface{}{
		"type":    "log",
		"level":   string(level),
		"ts":      time.Now().Unix(),
		"server":  serverName,
		"message": message,
	}

	// Add key-value pairs
	for i := 0; i+1 < len(kvs); i += 2 {
		key, ok := kvs[i].(string)
		if !ok {
			continue
		}
		value := kvs[i+1]

		// Convert errors to strings
		if err, ok := value.(error); ok {
			value = err.Error()
		}

		entry[key] = value
	}

	mu.Lock()
	encoder.Encode(entry)
	mu.Unlock()
}

// Debug logs a debug message
func Debug(message string, kvs ...interface{}) {
	log(LevelDebug, message, kvs...)
}

// Info logs an info message
func Info(message string, kvs ...interface{}) {
	log(LevelInfo, message, kvs...)
}

// Warn logs a warning message
func Warn(message string, kvs ...interface{}) {
	log(LevelWarn, message, kvs...)
}

// Error logs an error message
func Error(message string, kvs ...interface{}) {
	log(LevelError, message, kvs...)
}
