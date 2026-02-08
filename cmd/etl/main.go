package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	httpadapter "github.com/couchcryptid/storm-data-etl-service/internal/adapter/http"
	kafkaadapter "github.com/couchcryptid/storm-data-etl-service/internal/adapter/kafka"
	"github.com/couchcryptid/storm-data-etl-service/internal/config"
	"github.com/couchcryptid/storm-data-etl-service/internal/observability"
	"github.com/couchcryptid/storm-data-etl-service/internal/pipeline"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := observability.NewLogger(cfg)
	metrics := observability.NewMetrics()

	reader := kafkaadapter.NewReader(cfg, logger)
	writer := kafkaadapter.NewWriter(cfg, logger)

	extractor := pipeline.NewExtractor(reader, logger)
	transformer := pipeline.NewTransformer(logger)
	loader := pipeline.NewLoader(writer, logger)

	p := pipeline.New(extractor, transformer, loader, logger, metrics)

	srv := httpadapter.NewServer(cfg.HTTPAddr, p, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start HTTP server.
	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
		}
	}()

	// Start ETL pipeline.
	go func() {
		if err := p.Run(ctx); err != nil {
			logger.Error("pipeline error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "error", err)
	}
	if err := reader.Close(); err != nil {
		logger.Error("kafka reader close error", "error", err)
	}
	if err := writer.Close(); err != nil {
		logger.Error("kafka writer close error", "error", err)
	}

	logger.Info("shutdown complete")
}
