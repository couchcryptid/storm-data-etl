package pipeline

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
)

// MessageReader is the port for reading messages from a broker.
type MessageReader interface {
	ReadMessage(ctx context.Context) (domain.RawEvent, error)
	Close() error
}

// KafkaExtractor implements Extractor by delegating to a MessageReader.
type KafkaExtractor struct {
	reader MessageReader
	logger *slog.Logger
}

// NewExtractor creates a KafkaExtractor backed by the given reader.
func NewExtractor(reader MessageReader, logger *slog.Logger) *KafkaExtractor {
	return &KafkaExtractor{reader: reader, logger: logger}
}

func (e *KafkaExtractor) Extract(ctx context.Context) (domain.RawEvent, error) {
	return e.reader.ReadMessage(ctx)
}
