# Pixels Spark

[English](README.md) | [简体中文](README.zh-CN.md)

`pixels-spark` 是一个面向 Pixels CDC 流的 Spark Structured Streaming 连接器与 Delta Lake merge 管线。

它提供两类核心能力：

- 使用 `spark.readStream.format("pixels")` 从 Pixels RPC 服务读取 CDC 记录
- 从 Pixels metadata service 自动加载主键，并将 CDC 记录 merge 到 Delta Lake 表中

## 功能特性

- 自动从 Pixels metadata service 加载 schema
- 自动发现主键并做进程内 metadata 缓存
- 基于 Pixels RPC 轮询协议的 Spark Structured Streaming source
- 面向 `INSERT`、`UPDATE`、`DELETE`、`SNAPSHOT` 的 Delta Lake `MERGE`
- 默认 `hard delete`，保持目标 Delta 表 schema 与源 schema 一致
- 可选 `soft delete` 模式，适合保留删除痕迹的场景
- 提供 build、submit、validation、benchmark 辅助脚本

## 环境要求

- Linux 或其他类 Unix 环境
- Java 17
- Maven 3.x
- 带 `spark-submit` 的 Spark 3.5.x 发行版
- 可访问的 Pixels RPC 服务
- 可访问的 Pixels metadata service

根据实验环境不同，以下组件是可选的：

- MinIO 或其他兼容 S3 的对象存储
- Hive Metastore
- Trino
- Flink

## 安装

编译项目：

```bash
mvn -DskipTests compile
```

构建可部署的 shaded JAR：

```bash
./scripts/build-package.sh
```

生成的产物为：

```text
target/pixels-spark-0.1.jar
```

当前打包方式已经把 Delta Lake 运行时依赖打进了这个 JAR。使用该产物配合 `spark-submit` 时，不需要再额外传 `--packages io.delta:...`。

## 快速开始

### 1. 验证 Pixels source

```bash
mvn -q -DskipTests \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsCustomerPullTest \
  -Dexec.args="localhost 9091 pixels_bench savingaccount 0" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

### 2. 运行流式 source 烟测

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

### 3. 运行 Delta merge 作业

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

### 4. 校验 merge 后的 Delta 表

预览数据：

```bash
./scripts/preview-delta-table.sh /tmp/pixels-spark-savingaccount-delta 5 local[1]
```

检查主键唯一性：

```bash
./scripts/check-delta-primary-key.sh \
  localhost \
  18888 \
  pixels_bench \
  savingaccount \
  /tmp/pixels-spark-savingaccount-delta \
  local[1]
```

预期校验规则：

```text
row_count == distinct_pk_count
```

## 配置

配置优先级：

1. Spark options 或 CLI 参数
2. `PIXELS_SPARK_CONFIG`
3. `$PIXELS_HOME/etc/pixels-spark.properties`
4. classpath 中的 `pixels-spark.properties`
5. `ConfigFactory.Instance()` 从 `PIXELS_CONFIG` 或 `$PIXELS_HOME/etc/pixels.properties` 读取的值

最小 `pixels-spark.properties` 示例：

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

`delete-mode` 可选值：

- `hard`：物理删除命中的 Delta 行
- `soft`：增加 `_pixels_is_deleted` 和 `_pixels_deleted_at` 列，并标记为删除
- `ignore`：忽略删除事件

默认行为：

- `hard delete`
- 目标 Delta schema 与源 schema 保持一致

## 使用 spark-submit 运行

打包后可以直接提交 shaded JAR：

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

脚本会按以下顺序寻找 `spark-submit`：

- `SPARK_SUBMIT_BIN`
- `SPARK_HOME/bin/spark-submit`
- `PATH` 中的 `spark-submit`

## 文档

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

## 当前验证范围

当前实现已经验证过：

- 从 metadata service 加载 schema
- 从 metadata service 发现主键
- 从 Pixels RPC 服务读取流式数据
- 基于主键的 Delta merge 和主键唯一性校验
- 默认 `hard delete` 下目标表 schema 与源 schema 一致
- 可选 `soft delete` 模式下的建表路径

## 已知限制

- 当前 streaming source 仍使用本地 synthetic offset，而不是服务端 cursor
- 当前 merge 采用 micro-batch 语义，不是 Flink 风格的原生 changelog table
- 批内去重依赖 Pixels 事务 metadata 字段
- `soft delete` 是可选模式，启用后会故意改变目标表 schema
