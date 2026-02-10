package kafka

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl/internal/config"
	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// Writer produces messages to a Kafka topic.
// It implements pipeline.Loader.
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

// Load publishes a single transformed event to the sink Kafka topic.
func (w *Writer) Load(ctx context.Context, event domain.OutputEvent) error {
	msg := mapOutputEventToMessage(event)
	return w.writer.WriteMessages(ctx, msg)
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
