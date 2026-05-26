package db

import (
	"errors"
	"fmt"
)

// MaxDatabaseIDLength is the maximum allowed length for a database ID.
const MaxDatabaseIDLength = 40

// ErrInvalidDatabaseID is returned when a database ID fails validation.
var ErrInvalidDatabaseID = errors.New("invalid database ID")

// ErrNoServersAvailable is returned when routing can't find a healthy
// backend server to assign a database to.
var ErrNoServersAvailable = errors.New("no servers available")

// ValidateDatabaseID checks that a database ID is DNS-compatible:
//   - 1–40 characters
//   - Lowercase letters, digits, and hyphens only
//   - No leading or trailing hyphen
//   - No double-hyphen ("--") sequences (reserved for subdomain convention)
func ValidateDatabaseID(id string) error {
	if len(id) == 0 {
		return fmt.Errorf("%w: must not be empty", ErrInvalidDatabaseID)
	}
	if len(id) > MaxDatabaseIDLength {
		return fmt.Errorf("%w: must be %d characters or fewer", ErrInvalidDatabaseID, MaxDatabaseIDLength)
	}
	if id[0] == '-' {
		return fmt.Errorf("%w: must not start with a hyphen", ErrInvalidDatabaseID)
	}
	if id[len(id)-1] == '-' {
		return fmt.Errorf("%w: must not end with a hyphen", ErrInvalidDatabaseID)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' {
			continue
		}
		return fmt.Errorf("%w: invalid character %q (only lowercase letters, digits, and hyphens allowed)", ErrInvalidDatabaseID, string(c))
	}
	// "--" is reserved for the database--project subdomain convention.
	for i := 0; i < len(id)-1; i++ {
		if id[i] == '-' && id[i+1] == '-' {
			return fmt.Errorf("%w: must not contain \"--\"", ErrInvalidDatabaseID)
		}
	}
	return nil
}

// ParseDatabasePath parses "project/database" format.
func ParseDatabasePath(path string) (projectID, databaseID string, ok bool) {
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			projectID = path[:i]
			databaseID = path[i+1:]
			ok = projectID != "" && databaseID != ""
			return
		}
	}
	return "", "", false
}
