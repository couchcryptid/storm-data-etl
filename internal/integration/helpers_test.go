//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// startKafka spins up a Kafka container and returns the broker address.
// The container is terminated via t.Cleanup.
func startKafka(ctx context.Context, t *testing.T) string {
	t.Helper()
	kc, err := tcKafka.Run(ctx, "confluentinc/confluent-local:7.6.0")
	require.NoError(t, err, "start kafka container")
	t.Cleanup(func() { _ = kc.Terminate(ctx) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err, "get kafka brokers")
	return brokers[0]
}

// createTopic creates a single-partition topic on the given broker.
func createTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", broker)
	require.NoError(t, err, "dial kafka for topic creation")
	defer func() { _ = conn.Close() }()

	err = conn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err, "create topic %s", topic)
}

// loadMockData reads the mock CSV-style JSON records from the test fixtures.
func loadMockData(t *testing.T) []domain.RawCSVRecord {
	t.Helper()
	data, err := os.ReadFile("../../data/mock/storm_reports_240426_combined.json")
	require.NoError(t, err, "read mock data")
	var records []domain.RawCSVRecord
	require.NoError(t, json.Unmarshal(data, &records), "unmarshal mock data")
	return records
}

// discardLogger returns a logger that discards all output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
