# Native Delta Lake Deployment

This document summarizes the native Delta Lake deployment model used alongside `pixels-spark`.

The goal is to document the surrounding Delta Lake infrastructure required for end-to-end experiments, including object storage, metadata, and query services.

## Architecture

The reference deployment uses:

- MinIO for S3-compatible object storage
- Hive Metastore for table metadata
- Trino for query validation
- Flink for a native Delta writer example
- Spark for the Pixels CDC merge pipeline in this repository

Two common data paths are supported:

1. Native Delta validation path:

```text
Flink -> Delta Lake -> Hive Metastore -> Trino
```

2. Pixels CDC merge path:

```text
Pixels RPC -> Spark Structured Streaming -> Delta Lake -> Trino
```

## Reference Versions

| Component | Version | Purpose |
| --- | --- | --- |
| Delta Lake | 3.3.2 | Delta table storage and merge |
| Flink | 1.20.1 | Native Delta writer example |
| Trino | 466 | Query validation |
| Hive Metastore | 3.1.2 | Table metadata service |
| Hadoop Runtime | 3.3.6 | S3A compatibility |
| MinIO | `minio/minio:RELEASE.2025-02-28T09-55-16Z` | Object storage |

## Deployment Constraints

The validated deployment model is:

- MinIO in Docker
- Flink running natively
- Hive Metastore running natively
- Trino running natively
- Delta Lake tables stored in MinIO or another S3-compatible backend

## Prerequisites

- Linux x86_64
- Docker
- Maven
- `curl`
- `tar`
- `ss`
- Java 8 for Hive Metastore
- Java 17 for Spark and Flink jobs
- Java 23 for Trino 466

## Companion Setup

This repository focuses on the Spark merge side. For experiments that also require a local Delta infrastructure stack, set up the following services first:

1. MinIO
2. Hive Metastore
3. Trino
4. Optional Flink Delta writer

Once those are available, `pixels-spark` can target the same Delta storage backend for CDC-driven merge tests.

## Validation Goal

A correct infrastructure deployment should let you:

1. Write Delta tables to the configured object store
2. Register or discover those tables through Hive Metastore when required
3. Query them through Trino
4. Run Spark merge jobs from this repository against the same Delta storage

## Relationship to This Repository

`pixels-spark` is responsible for:

- Reading CDC records from the Pixels RPC service
- Loading source schema and primary keys from the Pixels metadata service
- Merging those records into Delta Lake tables

The external Delta infrastructure is responsible for:

- Object storage
- Table metadata
- Query access
- Cross-engine validation

For functional and benchmark workflows, see [Delta Lake Test Flow](DELTA_LAKE_TEST_FLOW.md).
