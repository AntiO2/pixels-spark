# Delta Lake Import Quickstart

This document keeps only the shortest path:

1. Start HMS and Trino
2. Re-import `sf10` into S3
3. Re-register the tables in Trino
4. Validate with Trino CLI
5. Start CDC updates
6. Open the monitor for overall CPU / memory / job status

## 1. Start the Base Services

Start HMS:

```bash
/home/ubuntu/disk1/opt/run/start-metastore.sh
```

Start Trino:

```bash
/home/ubuntu/disk1/opt/run/start-trino.sh
```

If Trino needs to query Delta tables on S3, confirm:

- `/home/ubuntu/disk1/opt/trino-server-466/etc/catalog/delta_lake.properties`

contains at least:

```properties
connector.name=delta_lake
hive.metastore.uri=thrift://127.0.0.1:9083
delta.register-table-procedure.enabled=true
delta.enable-non-concurrent-writes=true
fs.native-s3.enabled=true
s3.aws-access-key=...
s3.aws-secret-key=...
s3.region=us-east-2
s3.endpoint=https://s3.us-east-2.amazonaws.com
```

Template file:

- [etc/trino-delta_lake.properties.example](../etc/trino-delta_lake.properties.example)

## 2. Re-import `sf10` into S3

Project configuration:

- [etc/pixels-spark.properties](../etc/pixels-spark.properties)

Notes:

- both shell scripts and application code now read this same file by default
- defaults for `run-import-hybench-sf10.sh`, `run-import-hybench-sf1000.sh`, `run-cdc-hybench-sf10.sh`, and `run-single-cdc-foreground.sh` are centralized here
- avoid splitting the same operational settings across multiple `.env` files
- benchmark definition files:
  - [src/main/resources/benchmarks/hybench.properties](../src/main/resources/benchmarks/hybench.properties)
  - [src/main/resources/benchmarks/chbenchmark.properties](../src/main/resources/benchmarks/chbenchmark.properties)
- these files define table names, input file names, delimiters, Spark schemas, and primary-key columns
- ImportApp and CDC now share the same local definitions instead of fetching primary keys dynamically from the Pixels metadata service

Example table definition:

```properties
tables=customer,company
table.customer.file=customer.csv
table.customer.delimiter=,
table.customer.primary-keys=custID
table.customer.schema=custID:int,name:string,freshness_ts:timestamp
```

The `schema` field uses a comma-separated `column:type` format. The currently supported types are:

- `int`
- `long`
- `float`
- `double`
- `string`
- `date`
- `timestamp`
- `boolean`

Confirm the import-related settings:

```properties
pixels.spark.delta.enable-deletion-vectors=true
pixels.spark.import.csv.chunk-rows=2560000
pixels.spark.import.count-rows=false
```

Notes:

- `pixels.spark.delta.hash-bucket.count` is deprecated and should no longer be configured
- `_pixels_bucket_id` is no longer computed with Spark `pmod(hash(pk), x)`
- the authoritative bucket configuration now comes from `node.bucket.num` in `$PIXELS_HOME/etc/pixels.properties`
- both import and CDC compute bucket ids from canonical primary-key bytes -> `ByteString` -> `RetinaUtils`, matching the server

Run the full import:

```bash
./scripts/run-import-hybench-sf10.sh \
  /home/ubuntu/disk1/hybench_sf10 \
  s3a://home-zinuo/deltalake/hybench_sf10
```

Run the CHBenCHMark `w1` import:

```bash
./scripts/run-import-chbenchmark-w1.sh \
  /home/ubuntu/disk1/ch_w1 \
  s3a://home-zinuo/deltalake/chbenchmark_w1
```

Notes:

- the import uses `overwrite`
- the import writes a persistent `_pixels_bucket_id` column
- the bucket value is computed with the same server-side bucket algorithm, not Spark `hash()`
- new tables are created with `delta.enableDeletionVectors=true`
- imports do not run `count()` by default
- CSV data is read in chunks controlled by `pixels.spark.import.csv.chunk-rows`, then written to Delta chunk by chunk
- CDC source batch sizing can be controlled through `pixels.spark.source.max-rows-per-batch`, `pixels.spark.source.max-wait-ms-per-batch`, and `pixels.spark.source.empty-poll-sleep-ms`
- all import entrypoints now use the Java app:
  - `io.pixelsdb.spark.app.PixelsBenchmarkDeltaImportApp`

If you want DV enabled at table-creation time, the core Delta table property is:

```properties
delta.enableDeletionVectors=true
```

In this project, the recommended switch is:

```properties
pixels.spark.delta.enable-deletion-vectors=true
```

This applies to both:

- CSV-import table creation
- CDC auto-create table creation

## 3. Re-register the Tables in Trino

After a re-import or a partition change, re-register:

```bash
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute "CREATE SCHEMA IF NOT EXISTS delta_lake.hybench_sf10"

for table_name in customer company savingaccount checkingaccount transfer checking loanapps loantrans; do
  /home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
    --execute \"DROP TABLE IF EXISTS delta_lake.hybench_sf10.${table_name}\"
done

/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'customer', table_location => 's3://home-zinuo/deltalake/hybench_sf10/customer')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'company', table_location => 's3://home-zinuo/deltalake/hybench_sf10/company')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'savingaccount', table_location => 's3://home-zinuo/deltalake/hybench_sf10/savingaccount')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'checkingaccount', table_location => 's3://home-zinuo/deltalake/hybench_sf10/checkingaccount')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'transfer', table_location => 's3://home-zinuo/deltalake/hybench_sf10/transfer')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'checking', table_location => 's3://home-zinuo/deltalake/hybench_sf10/checking')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'loanapps', table_location => 's3://home-zinuo/deltalake/hybench_sf10/loanapps')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'loantrans', table_location => 's3://home-zinuo/deltalake/hybench_sf10/loantrans')\"
```

## 4. Query Validation

List the tables:

```bash
/home/ubuntu/disk1/opt/trino-cli/trino \
  --server http://127.0.0.1:8080 \
  --execute "SHOW TABLES FROM delta_lake.hybench_sf10"
```

Read one row:

```bash
/home/ubuntu/disk1/opt/trino-cli/trino \
  --server http://127.0.0.1:8080 \
  --execute "SELECT * FROM delta_lake.hybench_sf10.customer LIMIT 1"
```

## 5. Common Problems

If `SHOW TABLES` works but `SELECT` fails:

- check whether `delta_lake.properties` really contains the S3 settings
- confirm Trino has been restarted and loaded the new configuration

If you see:

```text
No factory for location: s3://.../_delta_log
```

the current Trino instance still cannot read S3.

If you see:

```text
Error getting snapshot for hybench_sf10.customer
```

the Trino-side S3 / Delta read configuration is still not effective.

If you need to enable DV on an existing table, you can run:

```bash
/home/ubuntu/disk1/opt/trino-cli/trino \
  --server http://127.0.0.1:8080 \
  --execute "ALTER TABLE delta_lake.hybench_sf10.customer SET PROPERTIES delta.enableDeletionVectors = true"
```

Or in Spark SQL:

```sql
ALTER TABLE delta.`s3a://home-zinuo/deltalake/hybench_sf10/customer`
SET TBLPROPERTIES ('delta.enableDeletionVectors'='true');
```

## 6. Start CDC Updates

Start the local dependency services first:

```bash
./scripts/start-local-cdc-stack.sh
```

Then start the full `sf10` CDC workload:

```bash
./scripts/run-cdc-hybench-sf10.sh
```

The local benchmark definitions used by CDC are controlled in [etc/pixels-spark.properties](../etc/pixels-spark.properties):

```properties
pixels.cdc.benchmark=hybench
```

To switch to CHBenCHMark:

```properties
pixels.cdc.benchmark=chbenchmark
```

This starts one Spark CDC job for each of:

- `customer`
- `company`
- `savingaccount`
- `checkingaccount`
- `transfer`
- `checking`
- `loanapps`
- `loantrans`

If you only want to validate source polling without executing the Delta merge, run:

```bash
./scripts/run-delta-merge.sh \
  --database pixels_bench \
  --table savingaccount \
  --rpc-host localhost \
  --rpc-port 9091 \
  --metadata-host localhost \
  --metadata-port 18888 \
  --mode polling \
  --trigger-mode processing-time \
  --trigger-interval "10 seconds" \
  --sink-mode noop
```

By default, CDC pulls all source buckets defined by `node.bucket.num` in `$PIXELS_HOME/etc/pixels.properties`; do not pass `--buckets` manually.

Notes:

- CDC source schemas come from the benchmark definition files
- CDC merge primary-key columns also come from the benchmark definition files
- CDC no longer depends on the Pixels metadata service for schema or primary-key definitions

## 7. Start Monitoring

Start metric collection:

```bash
./scripts/collect-cdc-metrics.sh
```

Start the web monitor:

```bash
python3 ./scripts/cdc_web_monitor.py
```

Open:

```text
http://127.0.0.1:8084
```

The monitor reports:

- dependency service status
- per-table CDC job status
- CPU / RSS / uptime for each Spark job
- machine-wide `load1`
- machine-wide used and available memory
- disk usage for the filesystem under `/tmp`

If you care about overall CPU and memory rather than just one job, look at the `System` section at the top of the page.
