package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/config"
	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// Reader consumes messages from a Kafka topic.
// It implements pipeline.BatchExtractor.
type Reader struct {
	reader        *kafkago.Reader
	flushInterval time.Duration
	logger        *slog.Logger
}

// NewReader creates a Kafka consumer for the configured source topic and group.
func NewReader(cfg *config.Config, logger *slog.Logger) *Reader {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaSourceTopic,
		GroupID:     cfg.KafkaGroupID,
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6, // 10 MB
	})
	return &Reader{reader: r, flushInterval: cfg.BatchFlushInterval, logger: logger}
}

// ExtractBatch fetches up to batchSize messages from Kafka.
// Each returned RawEvent includes a Commit callback for at-least-once delivery.
// Returns a partial batch when the flush interval elapses or the context is cancelled.
func (r *Reader) ExtractBatch(ctx context.Context, batchSize int) ([]domain.RawEvent, error) {
	batch := make([]domain.RawEvent, 0, batchSize)
	deadline := time.Now().Add(r.flushInterval)

	for len(batch) < batchSize {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			break
		}

		fetchCtx, cancel := context.WithTimeout(ctx, timeout)
		msg, err := r.reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if fetchCtx.Err() == context.DeadlineExceeded {
				break
			}
			if len(batch) > 0 {
				return batch, nil
			}
			return nil, err
		}

		raw := mapMessageToRawEvent(msg)
		raw.Commit = func(commitCtx context.Context) error {
			return r.reader.CommitMessages(commitCtx, msg)
		}
		batch = append(batch, raw)
	}

	return batch, nil
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
