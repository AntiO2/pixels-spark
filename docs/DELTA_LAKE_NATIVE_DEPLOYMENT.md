# Delta Lake 原生部署参考

这份文档把 `/home/antio2/projects/retina-deltalake` 里已经验证过的 Delta Lake 原生部署内容迁到 `pixels-spark` 仓库，方便在同一个仓库里同时查：

- Pixels CDC -> Spark -> Delta `MERGE`
- Delta Lake 本地对象存储 / Metastore / 查询层部署

这份文档的目标不是替代 `pixels-spark` 的 Spark merge 文档，而是保留一套可复现的 Delta Lake 基础设施搭建说明，便于后续在另一台机器上快速启动：

`Flink -> Delta Lake -> Hive Metastore -> Trino`

## 1. 部署约束

当前验证过的方式是：

1. `MinIO` 使用 Docker
2. `Flink` 原生运行
3. `Trino` 原生运行
4. `Delta Lake` 通过原生 Flink 作业写入
5. `Hive Metastore` 原生运行

## 2. 已验证版本

| 组件 | 版本 | 用途 |
| --- | --- | --- |
| Delta Lake | 3.3.2 | Flink 写入、Spark merge、Trino 查询 |
| Flink | 1.20.1 | 原生运行 Delta 写入作业 |
| Trino | 466 | 原生查询 Delta 表 |
| Hive Metastore | 3.1.2 | 保存 Delta 表注册信息 |
| Hadoop Runtime | 3.3.6 | Flink、HMS、Trino 的 S3A 兼容依赖 |
| MinIO | `minio/minio:RELEASE.2025-02-28T09-55-16Z` | 本地 S3 兼容对象存储 |

本机原始验证日期：`2026-04-03`

## 3. 数据流

原生 Delta 基础设施的数据流如下：

`Flink Job -> MinIO(warehouse/demo/orders) -> _delta_log + parquet -> Hive Metastore -> Trino`

如果叠加 `pixels-spark`，则数据流变成：

`Pixels RPC -> Spark Structured Streaming -> Delta MERGE -> MinIO/S3/HDFS -> Trino`

## 4. 前置条件

另一台机器至少需要满足：

1. Linux x86_64
2. 可联网访问 Maven Central、Apache 下载站、GitHub Releases、Docker Hub
3. 已安装 `docker`、`mvn`、`curl`、`tar`、`ss`
4. 可用 JDK：
   - Java 8：给 Hive Metastore
   - Java 17：给 Flink、Spark 和构建作业
   - Java 23：给 Trino 466
5. 当前用户对项目目录有读写权限

## 5. 原始仓库中的关键脚本

下面这些脚本都在原始目录 `/home/antio2/projects/retina-deltalake` 中，已经实际验证过：

- `scripts/install-native.sh`
- `scripts/build-job.sh`
- `scripts/start-native.sh`
- `scripts/stop-native.sh`
- `scripts/status-native.sh`
- `scripts/reset-demo-data.sh`
- `scripts/submit-flink-job.sh`
- `scripts/register-table.sh`

如果你的目标是重建原生 Delta/Flink/Trino 环境，最短命令序列是：

```bash
cd /home/antio2/projects/retina-deltalake
./scripts/install-native.sh
./scripts/build-job.sh
./scripts/start-native.sh
./scripts/reset-demo-data.sh
./scripts/submit-flink-job.sh
./scripts/register-table.sh
```

## 6. 标准验证结果

这套环境的标准验证结果是：

```text
"1","alice","128.5","PAID"
"2","bob","42.0","CREATED"
"3","carol","256.75","SHIPPED"
```

说明：

1. Delta 表已经成功写入 MinIO
2. Hive Metastore 已成功注册表
3. Trino 已成功查询 Delta 表

## 7. 为什么把这份文档放进 pixels-spark

原因有两个：

1. `pixels-spark` 的 merge 目标就是 Delta Lake，工程阅读时需要同时看到“如何写 Delta”与“如何部署 Delta 基础设施”
2. 后续做功能测试、吞吐测试、AP 测试时，Delta 基础设施是前置环境，不能只保留 Spark source / merge 文档

## 8. 与 pixels-spark 的关系

`pixels-spark` 本身负责：

- 从 Pixels RPC 拉 CDC 流
- 读 metadata service 拿 schema / 主键
- 用 Spark Structured Streaming 把 CDC merge 到 Delta

原生 Delta 基础设施负责：

- 对象存储
- metastore
- 查询层
- 原生 Flink Delta 写入样例

如果你的目标是做完整实验，建议先按这份文档搭 Delta 基础设施，再按 [README.md](/home/antio2/projects/pixels-spark/README.md) 跑 `pixels-spark` merge 流水线。
