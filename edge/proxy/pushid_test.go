package proxy

import (
	"testing"
	"time"
)

func TestGeneratePushID(t *testing.T) {
	id := GeneratePushID()

	// Should be 20 characters
	if len(id) != 20 {
		t.Errorf("Expected 20 characters, got %d: %q", len(id), id)
	}

	// Should only contain valid push characters
	for _, c := range id {
		if !isPushChar(byte(c)) {
			t.Errorf("Invalid character %q in push ID %q", c, id)
		}
	}
}

func TestGeneratePushIDUniqueness(t *testing.T) {
	seen := make(map[string]bool)

	// Generate 1000 IDs rapidly
	for i := 0; i < 1000; i++ {
		id := GeneratePushID()
		if seen[id] {
			t.Errorf("Duplicate push ID generated: %q", id)
		}
		seen[id] = true
	}
}

func TestGeneratePushIDChronologicalOrder(t *testing.T) {
	// IDs generated later should sort after earlier ones
	id1 := GeneratePushIDAt(time.Unix(1700000000, 0))
	id2 := GeneratePushIDAt(time.Unix(1700000001, 0))

	if id1 >= id2 {
		t.Errorf("Expected %q < %q (chronological order)", id1, id2)
	}
}

func TestGeneratePushIDSameMillisecond(t *testing.T) {
	// IDs generated in the same millisecond should still be unique and ordered
	now := time.Now()
	id1 := GeneratePushIDAt(now)
	id2 := GeneratePushIDAt(now)
	id3 := GeneratePushIDAt(now)

	if id1 == id2 || id2 == id3 || id1 == id3 {
		t.Errorf("IDs should be unique even in same millisecond: %q, %q, %q", id1, id2, id3)
	}

	// They should still be in order (incrementing random portion)
	if id1 >= id2 || id2 >= id3 {
		t.Errorf("Expected %q < %q < %q (same-ms ordering)", id1, id2, id3)
	}
}

func isPushChar(c byte) bool {
	for _, valid := range pushChars {
		if byte(valid) == c {
			return true
		}
	}
	return false
}
