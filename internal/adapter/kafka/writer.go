package kafka

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl/internal/config"
	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// Writer produces messages to a Kafka topic.
// It implements pipeline.BatchLoader.
type Writer struct {
	writer *kafkago.Writer
	logger *slog.Logger
}

// NewWriter creates a Kafka producer for the configured sink topic.
func NewWriter(cfg *config.Config, logger *slog.Logger) *Writer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaSinkTopic,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireAll,
	}
	return &Writer{writer: w, logger: logger}
}

// LoadBatch publishes multiple transformed events to the sink Kafka topic
// in a single WriteMessages call for efficiency.
func (w *Writer) LoadBatch(ctx context.Context, events []domain.OutputEvent) error {
	if len(events) == 0 {
		return nil
	}
	msgs := make([]kafkago.Message, len(events))
	for i, event := range events {
		msgs[i] = mapOutputEventToMessage(event)
	}
	return w.writer.WriteMessages(ctx, msgs...)
}

func (w *Writer) Close() error {
	return w.writer.Close()
}

func mapOutputEventToMessage(event domain.OutputEvent) kafkago.Message {
	headers := make([]kafkago.Header, 0, len(event.Headers))
	for k, v := range event.Headers {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return kafkago.Message{
		Key:     event.Key,
		Value:   event.Value,
		Headers: headers,
	}
}
