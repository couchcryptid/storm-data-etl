package config

import (
	"errors"
	"os"
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
}

// Load reads configuration from environment variables, applying defaults where unset.
func Load() (*Config, error) {
	shutdownStr := envOrDefault("SHUTDOWN_TIMEOUT", "10s")
	shutdownTimeout, err := time.ParseDuration(shutdownStr)
	if err != nil || shutdownTimeout <= 0 {
		return nil, errors.New("invalid SHUTDOWN_TIMEOUT")
	}

	cfg := &Config{
		KafkaBrokers:     parseBrokers(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaSourceTopic: envOrDefault("KAFKA_SOURCE_TOPIC", "raw-weather-reports"),
		KafkaSinkTopic:   envOrDefault("KAFKA_SINK_TOPIC", "transformed-weather-data"),
		KafkaGroupID:     envOrDefault("KAFKA_GROUP_ID", "storm-data-etl"),
		HTTPAddr:         envOrDefault("HTTP_ADDR", ":8080"),
		LogLevel:         envOrDefault("LOG_LEVEL", "info"),
		LogFormat:        envOrDefault("LOG_FORMAT", "json"),
		ShutdownTimeout:  shutdownTimeout,
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

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
