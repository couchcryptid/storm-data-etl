# Storm Data ETL Service

A Go service that consumes raw weather storm reports from Kafka, enriches and normalizes the data, and produces transformed events to a downstream Kafka topic. Part of the storm data pipeline.

## Pages

- [[Architecture]] -- System design, package layout, design decisions, and capacity
- [[Configuration]] -- Environment variables and validation
- [[Deployment]] -- Docker Compose setup and Docker image
- [[Development]] -- Build, test, lint, CI, and project conventions
- [[Enrichment Rules|Enrichment]] -- Transformation pipeline, normalization, and severity classification
