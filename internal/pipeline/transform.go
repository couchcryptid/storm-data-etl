package pipeline

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
)

// StormTransformer implements Transformer using domain transform functions.
type StormTransformer struct {
	logger *slog.Logger
}

// NewTransformer creates a StormTransformer.
func NewTransformer(logger *slog.Logger) *StormTransformer {
	return &StormTransformer{logger: logger}
}

func (t *StormTransformer) Transform(_ context.Context, raw domain.RawEvent) (domain.OutputEvent, error) {
	event, err := domain.ParseRawEvent(raw)
	if err != nil {
		return domain.OutputEvent{}, err
	}

	event = domain.EnrichStormEvent(event)

	return domain.SerializeStormEvent(event)
}
