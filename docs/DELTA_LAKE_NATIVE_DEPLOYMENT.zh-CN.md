# 原生 Delta Lake 部署

本文档说明如何为 `pixels-spark` 准备配套的 Delta Lake 原生基础设施。

目标是补齐端到端实验所需的外部组件，包括对象存储、metadata 和查询服务。

## 架构

参考部署包含：

- MinIO 作为兼容 S3 的对象存储
- Hive Metastore 作为表 metadata 服务
- Trino 作为查询验证引擎
- Flink 作为原生 Delta writer 示例
- Spark 作为本仓库中的 Pixels CDC merge 引擎

常见的两条数据路径：

1. 原生 Delta 验证路径

```text
Flink -> Delta Lake -> Hive Metastore -> Trino
```

2. Pixels CDC merge 路径

```text
Pixels RPC -> Spark Structured Streaming -> Delta Lake -> Trino
```

## 参考版本

| 组件 | 版本 | 用途 |
| --- | --- | --- |
| Delta Lake | 3.3.2 | Delta 表存储与 merge |
| Flink | 1.20.1 | 原生 Delta writer 示例 |
| Trino | 466 | 查询验证 |
| Hive Metastore | 3.1.2 | 表 metadata 服务 |
| Hadoop Runtime | 3.3.6 | S3A 兼容 |
| MinIO | `minio/minio:RELEASE.2025-02-28T09-55-16Z` | 对象存储 |

## 部署约束

当前验证过的部署模型为：

- MinIO 使用 Docker
- Flink 原生运行
- Hive Metastore 原生运行
- Trino 原生运行
- Delta Lake 表存储在 MinIO 或其他兼容 S3 的后端中

## 前置条件

- Linux x86_64
- Docker
- Maven
- `curl`
- `tar`
- `ss`
- Java 8 供 Hive Metastore 使用
- Java 17 供 Spark 和 Flink 作业使用
- Java 23 供 Trino 466 使用

## 配套环境说明

本仓库重点关注 Spark merge 侧。如果实验还需要一套本地 Delta 基础设施，请先准备：

1. MinIO
2. Hive Metastore
3. Trino
4. 可选的 Flink Delta writer

基础设施准备好之后，`pixels-spark` 就可以把 CDC merge 写到同一个 Delta 存储后端。

## 验证目标

正确部署后，应当可以做到：

1. 将 Delta 表写入目标对象存储
2. 在需要时通过 Hive Metastore 注册或发现这些表
3. 通过 Trino 查询这些表
4. 从本仓库运行 Spark merge 作业，写入同一套 Delta 存储

## 与本仓库的关系

`pixels-spark` 负责：

- 从 Pixels RPC 服务读取 CDC 记录
- 从 Pixels metadata service 加载源 schema 和主键
- 将 CDC 记录 merge 到 Delta Lake 表

外部 Delta 基础设施负责：

- 对象存储
- 表 metadata
- 查询访问
- 跨引擎验证

功能测试和 benchmark 流程请见 [Delta Lake 测试流程](DELTA_LAKE_TEST_FLOW.zh-CN.md)。
