# Local Spark / Trino / HMS Startup Commands

This document records the actual local commands used to start Spark, Trino, and HMS in the current environment.

## 1. Shared Environment

Load the shared environment first:

```bash
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
```

## 2. Start HMS

Use the existing script:

```bash
/home/ubuntu/disk1/opt/run/start-metastore.sh
```

This script:

- loads `pixels-delta-env.sh`
- sets `JAVA_HOME=$JAVA11_HOME`
- exports AWS credentials
- starts HMS in the background

Common checks:

```bash
cat /home/ubuntu/disk1/opt/run/metastore.pid
ss -ltn | rg ':9083'
tail -f /home/ubuntu/disk1/opt/logs/metastore.out
```

## 3. Start Trino

Use the existing script:

```bash
/home/ubuntu/disk1/opt/run/start-trino.sh
```

This script:

- loads `pixels-delta-env.sh`
- sets `JAVA_HOME=$JAVA23_HOME`
- starts Trino in the background

Common checks:

```bash
cat /home/ubuntu/disk1/opt/run/trino.pid
ss -ltn | rg ':8080'
tail -f /home/ubuntu/disk1/opt/logs/trino.out
```

Before Trino can query Delta tables on S3, confirm that the `delta_lake` catalog has S3 settings.

Live file:

```bash
/home/ubuntu/disk1/opt/trino-server-466/etc/catalog/delta_lake.properties
```

Repository template:

```bash
./etc/trino-delta_lake.properties.example
```

Minimal configuration:

```properties
connector.name=delta_lake
hive.metastore.uri=thrift://127.0.0.1:9083
delta.register-table-procedure.enabled=true
delta.enable-non-concurrent-writes=true
fs.native-s3.enabled=true
s3.aws-access-key=YOUR_AWS_ACCESS_KEY_ID
s3.aws-secret-key=YOUR_AWS_SECRET_ACCESS_KEY
s3.region=us-east-2
s3.endpoint=https://s3.us-east-2.amazonaws.com
```

Restart Trino after editing `delta_lake.properties`:

```bash
pkill -f 'trino-server-466' || true
/home/ubuntu/disk1/opt/run/start-trino.sh
```

## 4. Start Spark

This project usually does not run a standalone Spark service first. It normally submits jobs directly.

Load the environment and switch to Java 17:

```bash
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="$JAVA17_HOME"
```

Check the Spark version:

```bash
$SPARK_HOME/bin/spark-submit --version
```

Typical Spark job submission:

```bash
$SPARK_HOME/bin/spark-submit \
  --master local[4] \
  --driver-memory 20g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /home/ubuntu/disk1/projects/pixels-spark/scripts/import-benchmark-csv-to-delta-s3.py \
  /home/ubuntu/disk1/hybench_sf10 \
  s3a://home-zinuo/deltalake/hybench_sf10 \
  customer
```

## 5. Query with Trino CLI

After Trino starts, connect to the local coordinator:

```bash
/home/ubuntu/disk1/opt/trino-cli/trino --server 127.0.0.1:8080
```

Run a single SQL statement:

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SHOW CATALOGS"
```

List schemas in the Delta Lake catalog:

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SHOW SCHEMAS FROM delta_lake"
```

List tables in a schema:

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SHOW TABLES FROM delta_lake.hybench_sf10"
```

Query a table directly:

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SELECT * FROM delta_lake.hybench_sf10.customer LIMIT 10"
```

Inside the interactive CLI:

```sql
USE delta_lake.hybench_sf10;
SHOW TABLES;
SELECT count(*) FROM customer;
SELECT * FROM savingaccount LIMIT 10;
```

## 6. Optional: Start Spark Standalone

If you really want standalone master / worker processes first:

```bash
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="$JAVA17_HOME"

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077
```

Common checks:

```bash
ss -ltn | rg ':7077|:8081'
```

## 7. Most Common Startup Commands

For this project, the usual entry points are:

```bash
/home/ubuntu/disk1/opt/run/start-metastore.sh
/home/ubuntu/disk1/opt/run/start-trino.sh
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh && export JAVA_HOME="$JAVA17_HOME"
```

Then run the actual Spark job scripts, for example:

```bash
./scripts/run-import-hybench-sf10.sh
./scripts/run-cdc-hybench-sf10.sh
```

## 8. Start CDC and Monitoring

Start the local dependency stack first:

```bash
./scripts/start-local-cdc-stack.sh
```

This script checks and starts, when needed:

- HMS
- Trino
- Pixels metadata
- optional Pixels RPC
- Spark History Server

Start the full `sf10` CDC workload:

```bash
./scripts/run-cdc-hybench-sf10.sh
```

This starts one independent Spark CDC job per table.

Start metric collection:

```bash
./scripts/collect-cdc-metrics.sh
```

Start the read-only monitoring page:

```bash
python3 ./scripts/cdc_web_monitor.py
```

Default monitoring URL:

```text
http://127.0.0.1:8084
```

Raw JSON endpoint:

```text
http://127.0.0.1:8084/api/status
```

## 9. What the Monitor Shows

The monitor shows two kinds of information.

Service status:

- HMS
- Trino
- Pixels Metadata
- Pixels RPC
- Spark History

Job status:

- per-table `running` / `stopped`
- PID
- per-job CPU%
- per-job RSS memory
- uptime
- latest log summary

Overall system metrics come from `collect-cdc-metrics.sh`:

- `load1`
- `mem_used_mb`
- `mem_avail_mb`
- `disk_used_pct`

So the `System` panel at the top is machine-wide information, not only one Spark process.

Metric file locations:

- system CSV: `/tmp/hybench_sf10_cdc_metrics/system.csv`
- per-table JSON: `/tmp/hybench_sf10_cdc_metrics/<table>.json`
- per-table history CSV: `/tmp/hybench_sf10_cdc_metrics/<table>.csv`

Related logs:

- CDC job logs: `/tmp/hybench_sf10_cdc_logs/<table>.log`
- web monitor log: `/tmp/hybench_sf10_cdc_web.log`

If you want a CLI view of whole-machine CPU and memory, you can also use:

```bash
top
htop
pidstat -r -u -d 1
```
