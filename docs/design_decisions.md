# Architecture Design Decisions

## Why Medallion Architecture?

The platform separates raw ingestion, validated transformation and business-ready analytical data into Bronze, Silver and Gold layers.

## Why Delta Lake?

Delta Lake provides transactional storage, schema management, historical versions and reliable MERGE operations.

## Why Metadata-Driven Ingestion?

Rather than implementing independent ingestion logic for every source, pipeline behavior will be controlled through configuration metadata.

## Why Quarantine Invalid Data?

Invalid records should not be silently discarded. Quarantine allows the platform to preserve rejected records and identify the reason for failure.

## Why SCD Type 2?

Customer attributes such as city, segment and contact information can change over time. SCD Type 2 allows historical versions to be retained.

## Why Star Schema?

The Gold layer is designed for analytical workloads and separates measurable business events from descriptive dimensions.