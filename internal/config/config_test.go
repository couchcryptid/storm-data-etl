package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "raw-weather-reports", cfg.KafkaSourceTopic)
	assert.Equal(t, "transformed-weather-data", cfg.KafkaSinkTopic)
	assert.Equal(t, "storm-data-etl", cfg.KafkaGroupID)
	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.False(t, cfg.MapboxEnabled)
	assert.Empty(t, cfg.MapboxToken)
	assert.Equal(t, 5*time.Second, cfg.MapboxTimeout)
	assert.Equal(t, 1000, cfg.MapboxCacheSize)
}

func TestLoad_CustomEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_SOURCE_TOPIC", "custom-source")
	t.Setenv("KAFKA_SINK_TOPIC", "custom-sink")
	t.Setenv("KAFKA_GROUP_ID", "custom-group")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("SHUTDOWN_TIMEOUT", "30s")
	t.Setenv("MAPBOX_TOKEN", "pk.test-token")
	t.Setenv("MAPBOX_TIMEOUT", "10s")
	t.Setenv("MAPBOX_CACHE_SIZE", "500")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "custom-source", cfg.KafkaSourceTopic)
	assert.Equal(t, "custom-sink", cfg.KafkaSinkTopic)
	assert.Equal(t, "custom-group", cfg.KafkaGroupID)
	assert.Equal(t, ":9090", cfg.HTTPAddr)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "text", cfg.LogFormat)
	assert.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	assert.True(t, cfg.MapboxEnabled)
	assert.Equal(t, "pk.test-token", cfg.MapboxToken)
	assert.Equal(t, 10*time.Second, cfg.MapboxTimeout)
	assert.Equal(t, 500, cfg.MapboxCacheSize)
}

func TestLoad_InvalidShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "not-a-duration")
	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
}

func TestLoad_NegativeShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "-1s")
	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
}

func TestLoad_InvalidMapboxTimeout(t *testing.T) {
	t.Setenv("MAPBOX_TIMEOUT", "bad")
	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "MAPBOX_TIMEOUT")
}

func TestLoad_MapboxEnabledWithoutToken(t *testing.T) {
	t.Setenv("MAPBOX_ENABLED", "true")
	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "MAPBOX_TOKEN")
}

func TestLoad_MapboxTokenImpliesEnabled(t *testing.T) {
	t.Setenv("MAPBOX_TOKEN", "pk.test-token")
	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.MapboxEnabled)
}

func TestLoad_MapboxExplicitlyDisabled(t *testing.T) {
	t.Setenv("MAPBOX_TOKEN", "pk.test-token")
	t.Setenv("MAPBOX_ENABLED", "false")
	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.MapboxEnabled)
}

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"single broker", "localhost:9092", []string{"localhost:9092"}},
		{"multiple brokers", "a:1,b:2", []string{"a:1", "b:2"}},
		{"trims whitespace", " a , , b ", []string{"a", "b"}},
		{"empty string", "", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBrokers(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEnvOrDefault(t *testing.T) {
	t.Run("returns env value when set", func(t *testing.T) {
		t.Setenv("TEST_CONFIG_KEY", "custom")
		assert.Equal(t, "custom", envOrDefault("TEST_CONFIG_KEY", "default"))
	})

	t.Run("returns fallback when unset", func(t *testing.T) {
		assert.Equal(t, "fallback", envOrDefault("NONEXISTENT_KEY_FOR_TEST", "fallback"))
	})
}
