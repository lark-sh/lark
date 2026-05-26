package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/gowebpki/jcs"
)

// computeJCSHash computes the SHA-256 hash of the JCS-canonicalized JSON representation of a value.
// This is the Lark hash algorithm, used for ETag generation and conditional request (CAS) support.
// Returns the hash as a 64-character lowercase hex string.
// Must produce identical output to lark-server's compute_value_hash.
func computeJCSHash(value interface{}) (string, error) {
	// Marshal to JSON first
	jsonBytes, err := sonic.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal value: %w", err)
	}

	// Transform to JCS canonical form (RFC 8785)
	canonical, err := jcs.Transform(jsonBytes)
	if err != nil {
		return "", fmt.Errorf("canonicalize JSON: %w", err)
	}

	// Compute SHA-256 hash
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:]), nil
}
