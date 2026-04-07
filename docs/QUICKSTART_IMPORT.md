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

Confirm the bucket-count setting:

```properties
pixels.spark.delta.hash-bucket.count=16
```

Run the full import:

```bash
./scripts/run-import-hybench-sf10.sh \
  /home/ubuntu/disk1/hybench_sf10 \
  s3a://home-zinuo/deltalake/hybench_sf10
```

Notes:

- the import uses `overwrite`
- the import writes a persistent `_pixels_bucket_id` column
- the value is computed as `pmod(hash(pk), x)`

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
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'savingaccount', table_location => 's3://home-zinuo/deltalake/hybench_sf10/savingAccount')\"
/home/ubuntu/disk1/opt/trino-cli/trino --server http://127.0.0.1:8080 \
  --execute \"CALL delta_lake.system.register_table(schema_name => 'hybench_sf10', table_name => 'checkingaccount', table_location => 's3://home-zinuo/deltalake/hybench_sf10/checkingAccount')\"
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

## 6. Start CDC Updates

Start the local dependency services first:

```bash
./scripts/start-local-cdc-stack.sh
```

Then start the full `sf10` CDC workload:

```bash
./scripts/run-cdc-hybench-sf10.sh
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
