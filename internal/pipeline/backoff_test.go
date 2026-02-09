package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNextBackoff(t *testing.T) {
	tests := []struct {
		name    string
		current time.Duration
		max     time.Duration
		want    time.Duration
	}{
		{"doubles", 200 * time.Millisecond, 5 * time.Second, 400 * time.Millisecond},
		{"doubles again", 400 * time.Millisecond, 5 * time.Second, 800 * time.Millisecond},
		{"caps at max", 4 * time.Second, 5 * time.Second, 5 * time.Second},
		{"stays at max", 5 * time.Second, 5 * time.Second, 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, nextBackoff(tt.current, tt.max))
		})
	}
}

func TestSleepWithContext_Completes(t *testing.T) {
	ctx := context.Background()
	assert.True(t, sleepWithContext(ctx, 1*time.Millisecond))
}

func TestSleepWithContext_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, sleepWithContext(ctx, 1*time.Second))
}

func TestSleepWithContext_ZeroDuration(t *testing.T) {
	ctx := context.Background()
	assert.True(t, sleepWithContext(ctx, 0))
}
