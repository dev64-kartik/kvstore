package store

import (
	"errors"
	"testing"
)

func TestCheckKeyAndValueLimits(t *testing.T) {
	tests := []struct {
		name  string
		check func() error
		valid bool
	}{
		{"empty key", func() error { return CheckKey(nil) }, false},
		{"1-byte key", func() error { return CheckKey([]byte{0}) }, true},
		{"key at limit", func() error { return CheckKey(make([]byte, MaxKeySize)) }, true},
		{"key over limit", func() error { return CheckKey(make([]byte, MaxKeySize+1)) }, false},
		{"empty value", func() error { return CheckValue(nil) }, true},
		{"value at limit", func() error { return CheckValue(make([]byte, MaxValueSize)) }, true},
		{"value over limit", func() error { return CheckValue(make([]byte, MaxValueSize+1)) }, false},
	}
	for _, tt := range tests {
		err := tt.check()
		if tt.valid && err != nil {
			t.Errorf("%s: %v, want nil", tt.name, err)
		}
		if !tt.valid && !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v, want ErrInvalidArgument", tt.name, err)
		}
	}
}
