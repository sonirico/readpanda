package rp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertValidBatch(t *testing.T, batch Batch, err error) {
	t.Helper()
	require.NoError(t, err)
	require.NotNil(t, batch.Commit)
	require.NoError(t, batch.Commit(context.Background()))
}

func drainBatch(t *testing.T, c *MemoryConsumer) {
	t.Helper()
	batch, err := c.PollBatch(context.Background())
	require.NoError(t, err)
	require.NoError(t, batch.Commit(context.Background()))
}

func TestMemoryConsumer_PollBatch(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsMsgsOnPoll", func(t *testing.T) {
		t.Parallel()

		msgs := []Msg{
			{Topic: "t", Key: []byte("k1"), Value: []byte("v1")},
			{Topic: "t", Key: []byte("k2"), Value: []byte("v2")},
		}
		c := NewMemoryConsumer()
		c.SetMessages(msgs)

		batch, err := c.PollBatch(context.Background())

		assertValidBatch(t, batch, err)
		assert.Equal(t, msgs, batch.Msgs)
	})

	t.Run("ClearsQueueAfterPoll", func(t *testing.T) {
		t.Parallel()

		c := NewMemoryConsumer()
		c.SetMessages([]Msg{{Topic: "t", Key: []byte("k"), Value: []byte("v")}})
		drainBatch(t, c)

		second, err := c.PollBatch(context.Background())

		assertValidBatch(t, second, err)
		assert.Empty(t, second.Msgs)
	})

	t.Run("EmptyWhenNoMsgsConfigured", func(t *testing.T) {
		t.Parallel()

		c := NewMemoryConsumer()

		batch, err := c.PollBatch(context.Background())

		assertValidBatch(t, batch, err)
		assert.Empty(t, batch.Msgs)
	})
}
