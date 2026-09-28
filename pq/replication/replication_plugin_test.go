package replication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplicationPluginArguments(t *testing.T) {
	t.Run("postgres 13 proto version 1 omits logical messages", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '1'",
		}, replicationPluginArguments(1, 130000))
	})

	t.Run("postgres 13 proto version 2 omits logical messages and requests streaming", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '2'",
			"streaming 'true'",
		}, replicationPluginArguments(2, 139999))
	})

	t.Run("postgres 14 proto version 1 requests logical messages and not streaming", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '1'",
			"messages 'true'",
		}, replicationPluginArguments(1, logicalMessagesMinServerVersion))
	})

	t.Run("postgres 14 proto version 2 requests logical messages and streaming", func(t *testing.T) {
		assert.Equal(t, []string{
			"proto_version '2'",
			"messages 'true'",
			"streaming 'true'",
		}, replicationPluginArguments(2, 160002))
	})
}
