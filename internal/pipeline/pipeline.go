package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	"github.com/couchcryptid/storm-data-etl-service/internal/observability"
)

// Extractor reads a single raw event from the source.
type Extractor interface {
	Extract(ctx context.Context) (domain.RawEvent, error)
}

// Transformer converts a raw event into an output event.
type Transformer interface {
	Transform(ctx context.Context, raw domain.RawEvent) (domain.OutputEvent, error)
}

// Loader writes an output event to the destination.
type Loader interface {
	Load(ctx context.Context, event domain.OutputEvent) error
}

// Pipeline orchestrates the extract-transform-load loop.
type Pipeline struct {
	extractor   Extractor
	transformer Transformer
	loader      Loader
	logger      *slog.Logger
	metrics     *observability.Metrics
	ready       bool
}

// New creates a Pipeline with the given stages and observability.
func New(e Extractor, t Transformer, l Loader, logger *slog.Logger, metrics *observability.Metrics) *Pipeline {
	return &Pipeline{
		extractor:   e,
		transformer: t,
		loader:      l,
		logger:      logger,
		metrics:     metrics,
	}
}

// Ready reports whether the pipeline has successfully processed at least one message.
func (p *Pipeline) Ready() bool {
	return p.ready
}

// Run executes the ETL loop until the context is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.Info("pipeline started")
	p.metrics.PipelineRunning.Set(1)
	defer p.metrics.PipelineRunning.Set(0)

	backoff := 200 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("pipeline stopping", "reason", ctx.Err())
			return nil
		default:
		}

		start := time.Now()

		raw, err := p.extractor.Extract(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.logger.Error("extract failed", "error", err)
			if !sleepWithContext(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		p.metrics.MessagesConsumed.Inc()

		out, err := p.transformer.Transform(ctx, raw)
		if err != nil {
			p.logger.Warn("transform failed, skipping message",
				"error", err,
				"topic", raw.Topic,
				"partition", raw.Partition,
				"offset", raw.Offset,
			)
			p.metrics.TransformErrors.Inc()
			continue
		}

		if err := p.loader.Load(ctx, out); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.logger.Error("load failed", "error", err)
			if !sleepWithContext(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		p.metrics.MessagesProduced.Inc()

		if raw.Commit != nil {
			if err := raw.Commit(ctx); err != nil {
				p.logger.Warn("commit offset failed", "error", err, "topic", raw.Topic, "partition", raw.Partition, "offset", raw.Offset)
			}
		}

		p.metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
		backoff = 200 * time.Millisecond
		p.ready = true
	}
}

func nextBackoff(current, maxBackoff time.Duration) time.Duration {
	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
