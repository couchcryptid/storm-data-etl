package config

import (
	"errors"
	"strings"
	"time"

	"github.com/spf13/viper"
)

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

func Load() (*Config, error) {
	v := viper.New()
	v.SetDefault("KAFKA_BROKERS", "localhost:9092")
	v.SetDefault("KAFKA_SOURCE_TOPIC", "raw-weather-reports")
	v.SetDefault("KAFKA_SINK_TOPIC", "transformed-weather-data")
	v.SetDefault("KAFKA_GROUP_ID", "storm-data-etl")
	v.SetDefault("HTTP_ADDR", ":8080")
	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("LOG_FORMAT", "json")
	v.SetDefault("SHUTDOWN_TIMEOUT", "10s")
	v.AutomaticEnv()

	cfg := &Config{
		KafkaBrokers:     parseBrokers(v.GetString("KAFKA_BROKERS")),
		KafkaSourceTopic: v.GetString("KAFKA_SOURCE_TOPIC"),
		KafkaSinkTopic:   v.GetString("KAFKA_SINK_TOPIC"),
		KafkaGroupID:     v.GetString("KAFKA_GROUP_ID"),
		HTTPAddr:         v.GetString("HTTP_ADDR"),
		LogLevel:         v.GetString("LOG_LEVEL"),
		LogFormat:        v.GetString("LOG_FORMAT"),
		ShutdownTimeout:  v.GetDuration("SHUTDOWN_TIMEOUT"),
	}

	if cfg.ShutdownTimeout <= 0 {
		return nil, errors.New("invalid SHUTDOWN_TIMEOUT")
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
