package store

import (
	"errors"
	"fmt"
)

// The data model: what a key and a value may be, and the errors that describe
// a request rather than a failure. It lives here, not in localdb, because it
// outlives localdb: the Phase 3 FSM, the server and the WAL all use it.

// Size limits. Keys are 1 byte to MaxKeySize; values 0 bytes to MaxValueSize.
// They bound the largest WAL record, which the WAL reader relies on to tell a
// corrupt length from a real one.
const (
	MaxKeySize   = 4 << 10 // 4 KiB
	MaxValueSize = 1 << 20 // 1 MiB
)

var (
	// ErrNotFound: there is no value for the key.
	ErrNotFound = errors.New("key not found")
	// ErrInvalidArgument: a key or value is outside the size limits. A write
	// rejected with it was not applied.
	ErrInvalidArgument = errors.New("invalid argument")
)

// CheckKey returns an error wrapping ErrInvalidArgument unless key is 1 to
// MaxKeySize bytes.
func CheckKey(key []byte) error {
	if len(key) == 0 || len(key) > MaxKeySize {
		return fmt.Errorf("%w: key is %d bytes, want 1..%d", ErrInvalidArgument, len(key), MaxKeySize)
	}
	return nil
}

// CheckValue returns an error wrapping ErrInvalidArgument if value is longer
// than MaxValueSize. An empty value is valid.
func CheckValue(value []byte) error {
	if len(value) > MaxValueSize {
		return fmt.Errorf("%w: value is %d bytes, max %d", ErrInvalidArgument, len(value), MaxValueSize)
	}
	return nil
}
