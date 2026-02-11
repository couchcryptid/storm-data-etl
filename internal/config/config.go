package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all service settings, populated from environment variables.
type Config struct {
	KafkaBrokers     []string
	KafkaSourceTopic string
	KafkaSinkTopic   string
	KafkaGroupID     string
	HTTPAddr         string
	LogLevel         string
	LogFormat        string
	ShutdownTimeout  time.Duration

	BatchSize          int
	BatchFlushInterval time.Duration

	// Mapbox geocoding configuration.
	MapboxToken     string
	MapboxEnabled   bool
	MapboxTimeout   time.Duration
	MapboxCacheSize int
}

// Load reads configuration from environment variables, applying defaults where unset.
func Load() (*Config, error) {
	shutdownStr := envOrDefault("SHUTDOWN_TIMEOUT", "10s")
	shutdownTimeout, err := time.ParseDuration(shutdownStr)
	if err != nil || shutdownTimeout <= 0 {
		return nil, errors.New("invalid SHUTDOWN_TIMEOUT")
	}

	mapboxTimeoutStr := envOrDefault("MAPBOX_TIMEOUT", "5s")
	mapboxTimeout, err2 := time.ParseDuration(mapboxTimeoutStr)
	if err2 != nil || mapboxTimeout <= 0 {
		return nil, errors.New("invalid MAPBOX_TIMEOUT")
	}

	batchSize, err := parseBatchSize()
	if err != nil {
		return nil, err
	}

	flushInterval, err := parseBatchFlushInterval()
	if err != nil {
		return nil, err
	}

	mapboxCacheSize := parseMapboxCacheSize()

	mapboxToken := os.Getenv("MAPBOX_TOKEN")
	mapboxEnabled := mapboxToken != ""
	if v := os.Getenv("MAPBOX_ENABLED"); v != "" {
		mapboxEnabled = v == "true"
	}

	cfg := &Config{
		KafkaBrokers:       parseBrokers(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaSourceTopic:   envOrDefault("KAFKA_SOURCE_TOPIC", "raw-weather-reports"),
		KafkaSinkTopic:     envOrDefault("KAFKA_SINK_TOPIC", "transformed-weather-data"),
		KafkaGroupID:       envOrDefault("KAFKA_GROUP_ID", "storm-data-etl"),
		HTTPAddr:           envOrDefault("HTTP_ADDR", ":8080"),
		LogLevel:           envOrDefault("LOG_LEVEL", "info"),
		LogFormat:          envOrDefault("LOG_FORMAT", "json"),
		ShutdownTimeout:    shutdownTimeout,
		BatchSize:          batchSize,
		BatchFlushInterval: flushInterval,

		MapboxToken:     mapboxToken,
		MapboxEnabled:   mapboxEnabled,
		MapboxTimeout:   mapboxTimeout,
		MapboxCacheSize: mapboxCacheSize,
	}

	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS is required")
	}
	if cfg.KafkaSourceTopic == "" {
		return nil, errors.New("KAFKA_SOURCE_TOPIC is required")
	}
	if cfg.KafkaSinkTopic == "" {
		return nil, errors.New("KAFKA_SINK_TOPIC is required")
	}
	if cfg.MapboxEnabled && cfg.MapboxToken == "" {
		return nil, errors.New("MAPBOX_ENABLED is true but MAPBOX_TOKEN is not set")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseBatchSize() (int, error) {
	s := os.Getenv("BATCH_SIZE")
	if s == "" {
		return 50, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 1000 {
		return 0, errors.New("invalid BATCH_SIZE: must be 1-1000")
	}
	return n, nil
}

func parseBatchFlushInterval() (time.Duration, error) {
	s := envOrDefault("BATCH_FLUSH_INTERVAL", "500ms")
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, errors.New("invalid BATCH_FLUSH_INTERVAL: must be a positive duration")
	}
	return d, nil
}

func parseMapboxCacheSize() int {
	if s := os.Getenv("MAPBOX_CACHE_SIZE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 1000
}

func parseBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}
