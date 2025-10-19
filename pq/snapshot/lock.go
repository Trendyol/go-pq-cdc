package snapshot

import (
	"context"
	"fmt"
	"os"

	"github.com/go-playground/errors"
)

// tryAcquireCoordinatorLock attempts to acquire the PostgreSQL advisory lock for coordinator role
func (s *Snapshotter) tryAcquireCoordinatorLock(ctx context.Context, slotName string) (bool, error) {
	// Use PostgreSQL advisory lock
	// Hash the slot name to create a consistent lock ID
	lockID := hashString(slotName)

	query := fmt.Sprintf("SELECT pg_try_advisory_lock(%d)", lockID)
	results, err := s.execQuery(ctx, s.jobMetadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "acquire coordinator lock")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, errors.New("no lock result returned")
	}

	// PostgreSQL returns 't' for true, 'f' for false
	acquired := string(results[0].Rows[0][0]) == "t"
	return acquired, nil
}

// releaseCoordinatorLock releases the PostgreSQL advisory lock
func (s *Snapshotter) releaseCoordinatorLock(ctx context.Context, slotName string) error {
	lockID := hashString(slotName)
	query := fmt.Sprintf("SELECT pg_advisory_unlock(%d)", lockID)

	if err := s.execSQL(ctx, s.jobMetadataConn, query); err != nil {
		return errors.Wrap(err, "release coordinator lock")
	}

	return nil
}

// hashString creates a numeric hash for PostgreSQL advisory lock
func hashString(s string) int64 {
	var hash int64
	for i := 0; i < len(s); i++ {
		hash = hash*31 + int64(s[i])
	}
	// Keep positive
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// generateInstanceID generates a unique instance identifier
func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}
