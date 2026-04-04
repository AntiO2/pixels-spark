## Pixels Spark

`pixels-spark` 是一个纯 Spark 项目，提供两类能力：

- `spark.readStream.format("pixels")` 从 Pixels RPC 拉 CDC 流
- 基于 Pixels metadata service 的主键，把 CDC 流 `MERGE` 到 Delta Lake 当前态表

当前项目路径：

```text
/home/antio2/projects/pixels-spark
```

本文档按“另一台机器也能快速启动验证”的标准组织，默认目标是本地验证 `pixels_bench.savingaccount -> Delta`.

补充文档：

- [docs/DELTA_LAKE_NATIVE_DEPLOYMENT.md](/home/antio2/projects/pixels-spark/docs/DELTA_LAKE_NATIVE_DEPLOYMENT.md)
- [docs/DELTA_LAKE_TEST_FLOW.md](/home/antio2/projects/pixels-spark/docs/DELTA_LAKE_TEST_FLOW.md)

## 1. 当前能力

已经实现并实际验证过：

1. 从 `PIXELS_HOME/etc/pixels.properties` 自动读取基础配置
2. 从 `PIXELS_HOME/etc/pixels-spark.properties` 读取 `pixels-spark` 自己的配置
3. 从 Pixels metadata service 自动读取表 schema
4. 从 Pixels metadata service 自动读取主键信息并做进程内缓存
5. `spark.readStream.format("pixels")` 实际跑通
6. `readStream -> foreachBatch -> Delta MERGE` 实际跑通
7. 支持单列主键和复合主键生成 `MERGE ON`
8. `DELETE` 事件默认按真删除处理
9. 默认 `hard delete` 下，目标 Delta 表 schema 与源 schema 保持一致

当前确认过的真实结果：

```text
database=pixels_bench
table=savingaccount
primary_keys=[accountid]
row_count=131
distinct_pk_count=131
```

这说明：

- `savingaccount` 的主键来自 metadata service，不是手写
- 当前 Delta 表里没有主键重复
- sink 行为是当前态 merge，不是简单 append 堆积

另外，`soft delete` 路径也已经做过烟测，目标表 schema 已经确认带上：

- `_pixels_is_deleted`
- `_pixels_deleted_at`

## 2. 配置加载规则

配置优先级固定为：

1. Spark `.option(...)` 或 CLI 参数
2. `PIXELS_SPARK_CONFIG` 指向的文件
3. `$PIXELS_HOME/etc/pixels-spark.properties`
4. classpath 内置 `pixels-spark.properties`
5. `ConfigFactory.Instance()` 读取的 `$PIXELS_HOME/etc/pixels.properties`

### 2.1 Pixels 基础配置

`ConfigFactory.Instance()` 会从以下位置读取：

- `PIXELS_CONFIG`
- 否则 `$PIXELS_HOME/etc/pixels.properties`

这里至少要有 metadata service 的地址，例如：

```properties
metadata.server.host=localhost
metadata.server.port=18888
```

### 2.2 pixels-spark 自己的配置文件

推荐在：

```text
$PIXELS_HOME/etc/pixels-spark.properties
```

最小示例：

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

`pixels.spark.delta.delete.mode` 支持：

- `hard`
- `soft`
- `ignore`

## 3. 核心实现

关键类：

- `src/main/java/io/pixelsdb/spark/source/PixelsSourceProvider.java`
- `src/main/java/io/pixelsdb/spark/source/PixelsMetadataSchemaLoader.java`
- `src/main/java/io/pixelsdb/spark/metadata/PixelsTableMetadataRegistry.java`
- `src/main/java/io/pixelsdb/spark/merge/PixelsDeltaMergeJob.java`
- `src/main/java/io/pixelsdb/spark/app/PixelsDeltaMergeApp.java`

### 3.1 Source 输出列

业务列来自 metadata service，另外自动追加这些元数据列：

- `_pixels_op`
- `_pixels_txn_id`
- `_pixels_total_order`
- `_pixels_data_collection_order`

这些元数据列不会写入 Delta 当前态表，但会参与批内去重和 merge 顺序判定。

### 3.2 Merge 语义

`foreachBatch` 内部逻辑固定为：

1. 用 metadata service 读取主键列
2. 按主键分组
3. 用 `_pixels_total_order` 和 `_pixels_data_collection_order` 取每个主键在当前 micro-batch 的最新事件
4. `INSERT / SNAPSHOT / UPDATE` 进入 upsert 子集
5. `DELETE` 进入 delete 子集
6. 对 Delta 路径执行：
   - `WHEN MATCHED THEN UPDATE`
   - `WHEN NOT MATCHED THEN INSERT`
   - `WHEN MATCHED THEN DELETE` 或软删除更新

如果 metadata service 里没有主键，merge 作业会直接失败，不会偷偷退化成 append。

### 3.3 Delete 语义

`delete-mode=hard`

- 删除事件命中主键后，直接从 Delta 当前态表删除

`delete-mode=soft`

- 目标表自动带两列：
  - `_pixels_is_deleted BOOLEAN`
  - `_pixels_deleted_at TIMESTAMP`
- upsert 会自动写：
  - `_pixels_is_deleted=false`
  - `_pixels_deleted_at=NULL`
- delete 不删行，而是更新：
  - `_pixels_is_deleted=true`
  - `_pixels_deleted_at=current_timestamp()`

`delete-mode=ignore`

- delete 事件直接忽略

## 4. 构建

```bash
cd /home/antio2/projects/pixels-spark
mvn -DskipTests compile
```

我在 `2026-04-04` 已经实际编译通过。

## 5. 先验证 Source

### 5.1 直接拉 raw RPC 记录

```bash
cd /home/antio2/projects/pixels-spark
mvn -q -DskipTests \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsCustomerPullTest \
  -Dexec.args="localhost 9091 pixels_bench savingaccount 0" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

### 5.2 跑真正的 `readStream.format("pixels")`

```bash
cd /home/antio2/projects/pixels-spark
env JAVA_HOME=/home/antio2/.sdkman/candidates/java/17.0.14-jbr \
  SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_HOSTNAME=localhost \
  MAVEN_OPTS='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsSavingAccountStreamTest \
  -Dexec.args="localhost 9091 localhost 18888 pixels_bench savingaccount 0" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

## 6. 跑 Delta MERGE

下面这条命令会：

- 从 `pixels_bench.savingaccount`
- 拉 `bucket=0`
- 自动读 metadata schema
- 自动读 metadata 主键
- 把结果 merge 到本地 Delta 路径

```bash
cd /home/antio2/projects/pixels-spark
env JAVA_HOME=/home/antio2/.sdkman/candidates/java/17.0.14-jbr \
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

### 6.1 CLI 参数

`PixelsDeltaMergeApp` 支持：

- `--spark-master`
- `--database`
- `--table`
- `--buckets`
- `--rpc-host`
- `--rpc-port`
- `--metadata-host`
- `--metadata-port`
- `--target-path`
- `--checkpoint-location`
- `--trigger-mode`
- `--trigger-interval`
- `--auto-create-table`
- `--delete-mode`

`--trigger-mode` 支持：

- `once`
- `available-now`
- 其他值会按 `processingTime` 处理，并读取 `--trigger-interval`

### 6.2 软删除模式

```bash
cd /home/antio2/projects/pixels-spark
env JAVA_HOME=/home/antio2/.sdkman/candidates/java/17.0.14-jbr \
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
    --target-path /tmp/pixels-spark-savingaccount-delta-soft \
    --checkpoint-location /tmp/pixels-spark-savingaccount-soft-ckpt \
    --trigger-mode once \
    --delete-mode soft" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

这次烟测里，soft delete 目标表 schema 已经实际打印为：

```text
root
 |-- accountid: integer (nullable = true)
 |-- userid: integer (nullable = true)
 |-- balance: float (nullable = true)
 |-- isblocked: integer (nullable = true)
 |-- ts: timestamp (nullable = true)
 |-- freshness_ts: timestamp (nullable = true)
 |-- _pixels_is_deleted: boolean (nullable = true)
 |-- _pixels_deleted_at: timestamp (nullable = true)
```

## 7. 验证 Delta 输出

### 7.1 预览 Delta 表

```bash
cd /home/antio2/projects/pixels-spark
env JAVA_HOME=/home/antio2/.sdkman/candidates/java/17.0.14-jbr \
  SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_HOSTNAME=localhost \
  MAVEN_OPTS='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsDeltaTablePreviewApp \
  -Dexec.args="/tmp/pixels-spark-savingaccount-delta 5 local[1]" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

### 7.2 检查主键是否重复

这一步会：

- 再次从 metadata service 读取主键
- 统计 Delta 表总行数
- 统计主键去重后的行数

```bash
cd /home/antio2/projects/pixels-spark
env JAVA_HOME=/home/antio2/.sdkman/candidates/java/17.0.14-jbr \
  SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_HOSTNAME=localhost \
  MAVEN_OPTS='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsDeltaPrimaryKeyCheckApp \
  -Dexec.args="localhost 18888 pixels_bench savingaccount /tmp/pixels-spark-savingaccount-delta local[1]" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

预期输出类似：

```text
database=pixels_bench
table=savingaccount
delta_path=/tmp/pixels-spark-savingaccount-delta
primary_keys=[accountid]
row_count=131
distinct_pk_count=131
```

判断标准：

- `primary_keys` 不为空
- `row_count == distinct_pk_count`

## 8. 已知限制

1. 这是 Spark micro-batch merge，不是 Flink 风格的原生 `RowKind` 表
2. 当前 source offset 仍然是本地 synthetic offset，不是服务端 cursor
3. 当前批内去重依赖：
   - `_pixels_total_order`
   - `_pixels_data_collection_order`
   - `_pixels_txn_id`
4. 当前态 Delta 表默认不保留 `_pixels_*` 元数据列
5. `soft delete` 已经落地，但这次烟测批次里没有实际命中 delete 事件，所以验证到的是 schema 和写路径，不是 delete 命中后的行状态

## 9. 推荐验证顺序

在新机器上，按下面顺序最快：

1. 确认 `PIXELS_HOME/etc/pixels.properties` 里 metadata 地址正确
2. 可选写 `PIXELS_HOME/etc/pixels-spark.properties`
3. `mvn -DskipTests compile`
4. 跑 `PixelsSavingAccountStreamTest`
5. 跑 `PixelsDeltaMergeApp`
6. 跑 `PixelsDeltaPrimaryKeyCheckApp`

如果第 6 步看到：

```text
row_count == distinct_pk_count
```

就说明这条 `Pixels -> Spark -> Delta MERGE` 主链路已经跑通。

## 10. 标准交付与脚本

### 10.1 打包

```bash
cd /home/antio2/projects/pixels-spark
./scripts/build-package.sh
```

产物：

```text
/home/antio2/projects/pixels-spark/target/pixels-spark-0.1.jar
```

这是 shaded jar，可以直接用于 `spark-submit`。

### 10.2 spark-submit 脚本

如果机器上有 `spark-submit`，或者设置了：

- `SPARK_HOME`
- `SPARK_SUBMIT_BIN`

可以直接用：

```bash
./scripts/run-delta-merge.sh --database pixels_bench --table savingaccount ...
./scripts/preview-delta-table.sh /tmp/pixels-spark-savingaccount-delta 5 local[1]
./scripts/check-delta-primary-key.sh localhost 18888 pixels_bench savingaccount /tmp/pixels-spark-savingaccount-delta local[1]
```

### 10.3 验收脚本

```bash
./scripts/acceptance-delta-merge.sh \
  pixels_bench \
  savingaccount \
  0 \
  localhost \
  9091 \
  localhost \
  18888 \
  /tmp/pixels-spark-savingaccount-delta \
  /tmp/pixels-spark-savingaccount-ckpt
```

### 10.4 Benchmark 脚本

```bash
./scripts/benchmark-delta-merge.sh \
  3 \
  pixels_bench \
  savingaccount \
  0 \
  localhost \
  9091 \
  localhost \
  18888 \
  /tmp/pixels-spark-savingaccount-delta \
  /tmp/pixels-spark-benchmark-ckpt \
  --trigger-mode once
```
