# 本地 Spark / Trino / HMS 启动命令

本文档记录当前环境下本地启动 Spark、Trino、HMS 的实际命令。

## 1. 统一环境

启动前先加载环境：

```bash
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
```

## 2. 启动 HMS

直接使用现成脚本：

```bash
/home/ubuntu/disk1/opt/run/start-metastore.sh
```

这个脚本内部会：

- 加载 `pixels-delta-env.sh`
- 设置 `JAVA_HOME=$JAVA11_HOME`
- 注入 AWS 凭据
- 后台启动 HMS

常用检查：

```bash
cat /home/ubuntu/disk1/opt/run/metastore.pid
ss -ltn | rg ':9083'
tail -f /home/ubuntu/disk1/opt/logs/metastore.out
```

## 3. 启动 Trino

直接使用现成脚本：

```bash
/home/ubuntu/disk1/opt/run/start-trino.sh
```

这个脚本内部会：

- 加载 `pixels-delta-env.sh`
- 设置 `JAVA_HOME=$JAVA23_HOME`
- 后台启动 Trino

常用检查：

```bash
cat /home/ubuntu/disk1/opt/run/trino.pid
ss -ltn | rg ':8080'
tail -f /home/ubuntu/disk1/opt/logs/trino.out
```

Trino 查询 S3 上的 Delta 表前，还需要确认 `delta_lake` catalog 已经补齐 S3 配置。

线上实际文件：

```bash
/home/ubuntu/disk1/opt/trino-server-466/etc/catalog/delta_lake.properties
```

仓库里的模板文件：

```bash
./etc/trino-delta_lake.properties.example
```

最小配置示例：

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

修改完 `delta_lake.properties` 后需要重启 Trino：

```bash
pkill -f 'trino-server-466' || true
/home/ubuntu/disk1/opt/run/start-trino.sh
```

## 4. 启动 Spark

当前项目的主要用法不是先起独立 Spark 服务，而是直接提交作业。

先加载环境并切到 Java 17：

```bash
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="$JAVA17_HOME"
```

直接查看 Spark 版本：

```bash
$SPARK_HOME/bin/spark-submit --version
```

直接提交一个 Spark 作业的典型形式：

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

## 5. 使用 Trino CLI 查询

启动 Trino 后，可以直接用 CLI 连到本机 coordinator：

```bash
/home/ubuntu/disk1/opt/trino-cli/trino --server 127.0.0.1:8080
```

如果要直接执行一条 SQL：

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SHOW CATALOGS"
```

查询 Delta Lake catalog 里的 schema：

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SHOW SCHEMAS FROM delta_lake"
```

查询某个 schema 下的表：

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SHOW TABLES FROM delta_lake.hybench_sf10"
```

直接查表：

```bash
/home/ubuntu/disk1/opt/trino/bin/trino \
  --server 127.0.0.1:8080 \
  --execute "SELECT * FROM delta_lake.hybench_sf10.customer LIMIT 10"
```

进入交互式 CLI 后，也可以这样切 catalog 和 schema：

```sql
USE delta_lake.hybench_sf10;
SHOW TABLES;
SELECT count(*) FROM customer;
SELECT * FROM savingaccount LIMIT 10;
```

## 6. 可选：启动 Spark Standalone

如果你确实要先起 standalone master / worker，可以用 Spark 自带脚本：

```bash
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="$JAVA17_HOME"

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077
```

常用检查：

```bash
ss -ltn | rg ':7077|:8081'
```

## 7. 当前最常用的启动命令

如果只是跑本项目，通常只需要这三个入口：

```bash
/home/ubuntu/disk1/opt/run/start-metastore.sh
/home/ubuntu/disk1/opt/run/start-trino.sh
source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh && export JAVA_HOME="$JAVA17_HOME"
```

然后直接执行具体的 Spark 作业脚本，例如：

```bash
./scripts/run-import-hybench-sf10.sh
./scripts/run-cdc-hybench-sf10.sh
```

## 8. 启动 CDC 与监控

先启动本地依赖栈：

```bash
./scripts/start-local-cdc-stack.sh
```

这个脚本会检查并按需启动：

- HMS
- Trino
- Pixels metadata
- 可选的 Pixels RPC
- Spark History Server

启动 `sf10` 全表 CDC：

```bash
./scripts/run-cdc-hybench-sf10.sh
```

这个脚本会为每张表启动一个独立的 Spark CDC 作业。

启动指标采集：

```bash
./scripts/collect-cdc-metrics.sh
```

启动只读监控页：

```bash
python3 ./scripts/cdc_web_monitor.py
```

监控页默认地址：

```text
http://127.0.0.1:8084
```

Raw JSON 接口：

```text
http://127.0.0.1:8084/api/status
```

## 9. 监控里能看到什么

监控页会展示两类信息。

服务状态：

- HMS
- Trino
- Pixels Metadata
- Pixels RPC
- Spark History

作业状态：

- 每张表的 `running` / `stopped`
- PID
- 单作业 CPU%
- 单作业 RSS 内存
- 运行时长
- 最近一条日志摘要

整体系统信息来自 `collect-cdc-metrics.sh` 采样：

- `load1`
- `mem_used_mb`
- `mem_avail_mb`
- `disk_used_pct`

也就是监控页顶部的 `System` 区域显示的是整机概览，而不是单个 Spark 进程的局部信息。

采样文件位置：

- 系统总览 CSV：`/tmp/hybench_sf10_cdc_metrics/system.csv`
- 单表 JSON：`/tmp/hybench_sf10_cdc_metrics/<table>.json`
- 单表历史 CSV：`/tmp/hybench_sf10_cdc_metrics/<table>.csv`

相关日志位置：

- CDC 作业日志：`/tmp/hybench_sf10_cdc_logs/<table>.log`
- Web 监控日志：`/tmp/hybench_sf10_cdc_web.log`

如果你希望看整机 CPU / 内存的命令行视角，也可以直接使用：

```bash
top
htop
pidstat -r -u -d 1
```
