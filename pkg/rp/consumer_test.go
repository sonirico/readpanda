package rp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicConsumer_PollBatch(t *testing.T) {
	t.Parallel()

	t.Run("ClosedConsumer_ReturnsErrConsumerClosed", func(t *testing.T) {
		t.Parallel()

		c := &BasicConsumer{closed: true}

		_, err := c.PollBatch(context.Background())

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumerClosed)
	})
}
