// Push ID Generation
//
// Push IDs are 20-character strings that:
// - Are chronologically sortable (IDs generated later sort after earlier ones)
// - Are unique across clients (random component prevents collisions)
// - Use a 64-character alphabet that sorts correctly in ASCII order
//
// Format:
// - First 8 characters: timestamp (milliseconds since epoch)
// - Last 12 characters: random (incremented for same-millisecond IDs)
package proxy

import (
	"crypto/rand"
	"sync"
	"time"
)

const (
	// Characters used in push IDs, sorted for correct string ordering
	pushChars = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

	// Number of timestamp characters
	timestampLen = 8

	// Number of random characters
	randomLen = 12
)

var (
	// lastPushTime is the timestamp of the last push ID generated
	lastPushTime int64

	// lastRandChars stores the random portion from the last push ID
	// Used to increment when generating IDs in the same millisecond
	lastRandChars [randomLen]int

	pushMu sync.Mutex
)

// GeneratePushID generates a unique push ID.
// IDs are chronologically sortable and unique across clients.
func GeneratePushID() string {
	return GeneratePushIDAt(time.Now())
}

// GeneratePushIDAt generates a push ID for the given time.
// Useful for testing and deterministic ID generation.
func GeneratePushIDAt(t time.Time) string {
	pushMu.Lock()
	defer pushMu.Unlock()

	now := t.UnixMilli()
	duplicateTime := now == lastPushTime
	lastPushTime = now

	// Encode timestamp in first 8 characters
	id := make([]byte, timestampLen+randomLen)
	for i := timestampLen - 1; i >= 0; i-- {
		id[i] = pushChars[now%64]
		now /= 64
	}

	if duplicateTime {
		// Same millisecond: increment the random portion
		for i := randomLen - 1; i >= 0; i-- {
			if lastRandChars[i] < 63 {
				lastRandChars[i]++
				break
			}
			lastRandChars[i] = 0
		}
	} else {
		// New millisecond: generate fresh random portion
		randomBytes := make([]byte, randomLen)
		if _, err := rand.Read(randomBytes); err != nil {
			panic(err)
		}
		for i := 0; i < randomLen; i++ {
			lastRandChars[i] = int(randomBytes[i]) % 64
		}
	}

	// Encode random portion
	for i := 0; i < randomLen; i++ {
		id[timestampLen+i] = pushChars[lastRandChars[i]]
	}

	return string(id)
}
