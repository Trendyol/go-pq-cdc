package slot

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigInfoTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, 100 * time.Millisecond, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			cfg := Config{Name: "test_slot", SlotActivityCheckerInterval: 1000, InfoTimeout: timeout}
			if timeout < 0 {
				require.ErrorContains(t, cfg.Validate(), "infoTimeout")
				return
			}
			require.NoError(t, cfg.Validate())
			s := NewSlot("", "", cfg, nil, nil)
			t.Cleanup(func() { s.Close(context.Background()) })
			want := timeout
			if want == 0 {
				want = DefaultInfoTimeout
			}
			require.Equal(t, want, s.cfg.InfoTimeout)
		})
	}
}
