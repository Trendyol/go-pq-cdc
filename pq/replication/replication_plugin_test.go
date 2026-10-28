package replication

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationPluginArguments(t *testing.T) {
	t.Run("auto omits logical messages on postgres 13", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '1'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 1}, 13, true))
	})

	t.Run("auto omits logical messages and requests streaming on postgres 13 proto version 2", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '2'",
			"streaming 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 2}, 13, true))
	})

	t.Run("auto requests logical messages without streaming on postgres 14 proto version 1", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '1'",
			"messages 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 1}, logicalMessagesMinMajorVersion, true))
	})

	t.Run("auto requests logical messages and streaming on postgres 16 proto version 2", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '2'",
			"messages 'true'",
			"streaming 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 2}, 16, true))
	})

	t.Run("off keeps the option off a server that supports it", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '2'",
			"streaming 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 2, LogicalMessages: slot.LogicalMessagesOff}, 16, true))
	})

	t.Run("on requests the option", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '1'",
			"messages 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 1, LogicalMessages: slot.LogicalMessagesOn}, 14, true))
	})

	t.Run("on a server without the option fails startup", func(t *testing.T) {
		_, err := replicationPluginArguments(StartOptions{ProtoVersion: 2, LogicalMessages: slot.LogicalMessagesOn}, 13, true)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "PostgreSQL 14 or newer")
	})

	t.Run("unknown version falls back to the protocol version", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '1'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 1}, 0, false))
		assert.Equal(t, []string{
			"proto_version '2'",
			"messages 'true'",
			"streaming 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 2}, 0, false))
	})

	t.Run("off ignores an unknown version", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '2'",
			"streaming 'true'",
		}, mustPluginArguments(t, StartOptions{ProtoVersion: 2, LogicalMessages: slot.LogicalMessagesOff}, 0, false))
	})

	t.Run("on an unknown version fails startup", func(t *testing.T) {
		_, err := replicationPluginArguments(StartOptions{ProtoVersion: 1, LogicalMessages: slot.LogicalMessagesOn}, 0, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "known server version")
	})
}

// mustPluginArguments unwraps the error for the cases that must succeed, so a
// failure points at the arguments rather than at an ignored error.
func mustPluginArguments(t *testing.T, opts StartOptions, serverMajorVersion int, versionKnown bool) []string {
	t.Helper()

	args, err := replicationPluginArguments(opts, serverMajorVersion, versionKnown)
	require.NoError(t, err)

	return args
}
