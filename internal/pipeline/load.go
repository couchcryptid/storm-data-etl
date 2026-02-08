package pipeline

import (
	"context"

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
}

// NewLoader creates a KafkaLoader backed by the given writer.
func NewLoader(writer MessageWriter) *KafkaLoader {
	return &KafkaLoader{writer: writer}
}

func (l *KafkaLoader) Load(ctx context.Context, event domain.OutputEvent) error {
	return l.writer.WriteMessage(ctx, event)
}
