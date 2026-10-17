package replication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOnlineVisibilityDoesNotRestrictMajor(t *testing.T) {
	for _, version := range []string{"140015", "150007", "160003", "170007", "180003", "190000", "invalid"} {
		t.Run(version, func(t *testing.T) {
			g := &onlineVisibilityGate{source: onlineOptions().Source, timeout: time.Second, query: func(context.Context, string) ([]string, error) {
				return []string{"f", "db", "123", "1", "10:100:", version}, nil
			}}
			_, err := g.poll(context.Background())
			if version == "invalid" {
				if !errors.Is(err, ErrVisibilityGuard) {
					t.Fatalf("malformed version should be a guard failure: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
