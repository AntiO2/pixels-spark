# Pixels Spark

[English](README.md) | [简体中文](README.zh-CN.md)

`pixels-spark` is a Spark Structured Streaming connector and Delta Lake merge pipeline for Pixels CDC streams.

It provides two core capabilities:

- Read CDC records from the Pixels RPC service with `spark.readStream.format("pixels")`
- Merge those CDC records into a Delta Lake table using primary keys loaded from the Pixels metadata service

## Features

- Automatic schema loading from the Pixels metadata service
- Automatic primary-key discovery and in-process metadata caching
- Spark Structured Streaming source based on the Pixels RPC polling protocol
- Delta Lake `MERGE` pipeline for `INSERT`, `UPDATE`, `DELETE`, and `SNAPSHOT`
- Default `hard delete` behavior that keeps the target Delta schema aligned with the source schema
- Optional `soft delete` mode for audit-oriented workflows
- Packaged helper scripts for build, submit, validation, and benchmark runs

## Requirements

- Linux or another Unix-like environment
- Java 17
- Maven 3.x
- A Spark 3.5.x distribution with `spark-submit`
- Access to a running Pixels RPC service
- Access to a running Pixels metadata service

Optional, depending on the experiment:

- MinIO or S3-compatible object storage
- Hive Metastore
- Trino
- Flink

## Installation

Build the project:

```bash
mvn -DskipTests compile
```

Build a shaded deployment JAR:

```bash
./scripts/build-package.sh
```

The packaged artifact is:

```text
target/pixels-spark-0.1.jar
```

This JAR already includes the required Delta Lake runtime dependencies for the current packaging model. If you submit this artifact with `spark-submit`, you do not need an extra `--packages io.delta:...` argument.

## Quick Start

### 1. Verify the Pixels source

```bash
mvn -q -DskipTests \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsCustomerPullTest \
  -Dexec.args="localhost 9091 pixels_bench savingaccount 0" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

### 2. Run a streaming source smoke test

```bash
env JAVA_HOME=/path/to/java17 \
  SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_HOSTNAME=localhost \
  MAVEN_OPTS='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsSavingAccountStreamTest \
  -Dexec.args="localhost 9091 localhost 18888 pixels_bench savingaccount 0" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

### 3. Run a Delta merge job

```bash
env JAVA_HOME=/path/to/java17 \
  SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_HOSTNAME=localhost \
  MAVEN_OPTS='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsDeltaMergeApp \
  -Dexec.args="--spark-master local[1] \
    --database pixels_bench \
    --table savingaccount \
    --buckets 0 \
    --rpc-host localhost \
    --rpc-port 9091 \
    --metadata-host localhost \
    --metadata-port 18888 \
    --target-path /tmp/pixels-spark-savingaccount-delta \
    --checkpoint-location /tmp/pixels-spark-savingaccount-ckpt \
    --trigger-mode once" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

### 4. Validate the merged Delta table

Preview the table:

```bash
./scripts/preview-delta-table.sh /tmp/pixels-spark-savingaccount-delta 5 local[1]
```

Check primary-key uniqueness:

```bash
./scripts/check-delta-primary-key.sh \
  localhost \
  18888 \
  pixels_bench \
  savingaccount \
  /tmp/pixels-spark-savingaccount-delta \
  local[1]
```

Expected validation rule:

```text
row_count == distinct_pk_count
```

## Configuration

Configuration precedence:

1. Spark options or CLI arguments
2. `PIXELS_SPARK_CONFIG`
3. `$PIXELS_HOME/etc/pixels-spark.properties`
4. classpath `pixels-spark.properties`
5. `ConfigFactory.Instance()` values from `PIXELS_CONFIG` or `$PIXELS_HOME/etc/pixels.properties`

Minimal `pixels-spark.properties` example:

```properties
pixels.spark.rpc.host=localhost
pixels.spark.rpc.port=9091
pixels.spark.metadata.host=localhost
pixels.spark.metadata.port=18888
pixels.spark.delta.auto-create=true
pixels.spark.delta.delete.mode=hard
pixels.spark.delta.trigger.mode=once
pixels.spark.delta.trigger.interval=0 seconds
```

Delete mode options:

- `hard`: physically delete matched rows from the Delta table
- `soft`: add `_pixels_is_deleted` and `_pixels_deleted_at` columns and mark rows as deleted
- `ignore`: ignore delete events

Default behavior:

- `hard delete`
- target Delta schema remains aligned with the source schema

## Running with spark-submit

After packaging, you can submit the shaded JAR directly:

```bash
./scripts/run-delta-merge.sh \
  --database pixels_bench \
  --table savingaccount \
  --buckets 0 \
  --rpc-host localhost \
  --rpc-port 9091 \
  --metadata-host localhost \
  --metadata-port 18888 \
  --target-path /tmp/pixels-spark-savingaccount-delta \
  --checkpoint-location /tmp/pixels-spark-savingaccount-ckpt \
  --trigger-mode once
```

The helper scripts look for `spark-submit` in this order:

- `SPARK_SUBMIT_BIN`
- `SPARK_HOME/bin/spark-submit`
- `spark-submit` from `PATH`

## Documentation

English:

- [Local Startup Commands](docs/STARTUP_COMMANDS.md)
- [Delta Lake Import Quickstart](docs/QUICKSTART_IMPORT.md)
- [Native Delta Lake Deployment](docs/DELTA_LAKE_NATIVE_DEPLOYMENT.md)
- [Delta Lake Test Flow](docs/DELTA_LAKE_TEST_FLOW.md)

简体中文:

- [本地 Spark / Trino / HMS 启动命令](docs/STARTUP_COMMANDS.zh-CN.md)
- [Delta Lake 导入快速指南](docs/QUICKSTART_IMPORT.zh-CN.md)
- [原生 Delta Lake 部署](docs/DELTA_LAKE_NATIVE_DEPLOYMENT.zh-CN.md)
- [Delta Lake 测试流程](docs/DELTA_LAKE_TEST_FLOW.zh-CN.md)

## Validation Status

The current implementation has been validated for:

- Pixels schema loading from the metadata service
- Primary-key discovery from the metadata service
- Streaming source reads from the Pixels RPC service
- Delta merge with primary-key uniqueness validation
- Default `hard delete` behavior with source-schema-aligned target tables
- Optional `soft delete` table creation path

## Limitations

- The streaming source still uses a synthetic local offset rather than a server-side cursor
- The current merge pipeline uses micro-batch semantics, not Flink-style native changelog tables
- Batch-level deduplication relies on Pixels transaction metadata fields
- `soft delete` is optional and changes the target table schema by design
