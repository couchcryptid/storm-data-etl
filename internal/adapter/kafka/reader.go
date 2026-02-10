package kafka

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl/internal/config"
	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// Reader consumes messages from a Kafka topic.
// It implements pipeline.Extractor.
type Reader struct {
	reader *kafkago.Reader
	logger *slog.Logger
}

// NewReader creates a Kafka consumer for the configured source topic and group.
func NewReader(cfg *config.Config, logger *slog.Logger) *Reader {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaSourceTopic,
		GroupID:  cfg.KafkaGroupID,
		MinBytes: 1,
		MaxBytes: 10e6, // 10 MB
	})
	return &Reader{reader: r, logger: logger}
}

// Extract fetches a single message from Kafka without auto-committing the offset.
// The returned RawEvent includes a Commit callback that the caller must invoke
// after successful processing to achieve at-least-once delivery semantics.
func (r *Reader) Extract(ctx context.Context) (domain.RawEvent, error) {
	msg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return domain.RawEvent{}, err
	}

	raw := mapMessageToRawEvent(msg)
	raw.Commit = func(commitCtx context.Context) error {
		return r.reader.CommitMessages(commitCtx, msg)
	}

	return raw, nil
}

func (r *Reader) Close() error {
	return r.reader.Close()
}

func mapMessageToRawEvent(msg kafkago.Message) domain.RawEvent {
	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	return domain.RawEvent{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Time,
	}
}
