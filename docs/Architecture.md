# Architecture

## Overview

The Storm Data ETL Service is a single-binary Go application that reads raw storm reports from Kafka, enriches them, and writes the results back to Kafka. It follows a hexagonal (ports and adapters) architecture.

## Data Flow

```
Kafka Source Topic            ETL Service                    Kafka Sink Topic
(raw-weather-reports)                                       (transformed-weather-data)
                        +-------------------------+
  JSON storm report --> | Extract (KafkaReader)   |
                        |           |             |
                        | Transform (Domain Logic)|
                        |   - Parse JSON          |
                        |   - Normalize fields    |
                        |   - Enrich (severity,   |
                        |     location, office)   |
                        |           |             |
                        | Load (KafkaWriter)      | --> Enriched JSON event
                        +-------------------------+
                                |
                         HTTP Server (:8080)
                          /healthz  /readyz  /metrics
```

## Package Layout

### `cmd/etl`

Application entry point. Wires together configuration, adapters, pipeline stages, and the HTTP server. Manages signal-based graceful shutdown.

### `internal/domain`

Pure domain logic with no infrastructure dependencies.

- **`event.go`** -- Domain types: `RawEvent`, `StormEvent`, `OutputEvent`, `Location`, `Geo`
- **`transform.go`** -- All transformation and enrichment functions: parsing, normalization, severity derivation, location parsing, serialization
- **`clock.go`** -- Swappable clock for deterministic testing

### `internal/pipeline`

Orchestration layer that defines the ETL interfaces and loop.

- **`pipeline.go`** -- `Extractor`, `Transformer`, and `Loader` interfaces. The `Pipeline` struct runs the continuous extract-transform-load loop with backoff on failure.
- **`transform.go`** -- `StormTransformer` adapts domain functions to the `Transformer` interface

### `internal/adapter/kafka`

Kafka infrastructure adapters that directly implement the pipeline's `Extractor` and `Loader` interfaces.

- **`reader.go`** -- Wraps `segmentio/kafka-go` Reader with explicit offset commit (consumer group mode). Implements `pipeline.Extractor`.
- **`writer.go`** -- Wraps `segmentio/kafka-go` Writer with `RequireAll` acks. Implements `pipeline.Loader`.

### `internal/adapter/http`

HTTP server for operational endpoints.

- `/healthz` -- Liveness: always 200
- `/readyz` -- Readiness: 200 after at least one message processed, 503 otherwise
- `/metrics` -- Prometheus handler

### `internal/observability`

- **`logging.go`** -- Configurable structured logger (`slog`) with JSON or text output
- **`metrics.go`** -- Prometheus counter, histogram, and gauge definitions

### `internal/config`

Environment-based configuration using `os.Getenv` with sensible defaults and validation.

## Design Decisions

### Hexagonal Architecture

Infrastructure adapters (Kafka, HTTP) are separated from domain logic via interfaces defined in the `pipeline` package. This allows:

- Unit testing the pipeline with mock extractors, transformers, and loaders
- Swapping Kafka for another broker without touching domain or pipeline code

### Explicit Offset Commit

The Kafka reader uses `FetchMessage` + manual `CommitMessages` rather than auto-commit. Offsets are committed only after the message has been successfully transformed and loaded, providing at-least-once delivery semantics.

### Backoff Strategy

The pipeline uses exponential backoff (200ms to 5s) on extract or load failures to avoid hammering a degraded broker. Backoff resets immediately after a successful extract.

### Graceful Shutdown

The main function uses `signal.NotifyContext` to capture `SIGINT`/`SIGTERM`. On shutdown:

1. The pipeline loop exits via context cancellation
2. The HTTP server drains connections within the configured timeout
3. Kafka reader and writer are closed

### Thread Safety

The `Pipeline.ready` flag uses `atomic.Bool` since it is written by the pipeline goroutine and read by the HTTP readiness handler concurrently.
