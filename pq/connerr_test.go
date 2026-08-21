package pq

import (
	"fmt"
	"io"
	"syscall"
	"testing"
)

func TestIsRecoverableConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "EOF", err: io.EOF, want: true},
		{name: "conn closed", err: fmt.Errorf("conn closed"), want: true},
		{name: "unrelated", err: fmt.Errorf("syntax error"), want: false},
		{name: "ECONNRESET", err: syscall.ECONNRESET, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRecoverableConnectionError(tt.err); got != tt.want {
				t.Fatalf("IsRecoverableConnectionError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
