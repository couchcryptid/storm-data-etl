package pipeline

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
)

// MessageWriter is the port for writing messages to a broker.
type MessageWriter interface {
	WriteMessage(ctx context.Context, event domain.OutputEvent) error
	Close() error
}

// KafkaLoader implements Loader by delegating to a MessageWriter.
type KafkaLoader struct {
	writer MessageWriter
	logger *slog.Logger
}

// NewLoader creates a KafkaLoader backed by the given writer.
func NewLoader(writer MessageWriter, logger *slog.Logger) *KafkaLoader {
	return &KafkaLoader{writer: writer, logger: logger}
}

func (l *KafkaLoader) Load(ctx context.Context, event domain.OutputEvent) error {
	return l.writer.WriteMessage(ctx, event)
}
