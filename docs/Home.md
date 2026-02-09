# Storm Data ETL Service

A Go service that consumes raw weather storm reports from Kafka, enriches and normalizes the data, and produces transformed events to a downstream Kafka topic. Part of the storm data pipeline.

## Pages

- [[Architecture]] -- System design, package layout, and design decisions
- [[Configuration]] -- Environment variables and validation
- [[Deployment]] -- Docker Compose setup and production considerations
- [[Development]] -- Build, test, lint, CI, and project conventions
- [[Enrichment Rules|Enrichment]] -- Transformation pipeline, normalization, and severity classification
- [[Performance]] -- Theoretical throughput, scaling, and bottleneck analysis
