# Hudi Spark Thrift Server Quick Start

这份文档只包含最小步骤：启动、注册、查询。

## 1) 启动

```bash
cd /home/ubuntu/disk1/pixels-spark-hudi-thriftserver-0.1-20260416T122600Z
GLUE_ENABLED=false ./bin/start-thriftserver.sh
```

检查端口：

```bash
ss -ltnp | rg ':10000'
```

停止服务：

```bash
./bin/stop-thriftserver.sh
```

## 2) 注册表

一次性注册当前库下全部表：

```bash
cd /home/ubuntu/disk1/pixels-spark-hudi-thriftserver-0.1-20260416T122600Z
./bin/init-all-hudi-tables.sh
```

如果只想注册 `loantrans`：

```bash
/home/ubuntu/disk1/pixels-spark-hudi-thriftserver-0.1-20260416T122600Z/spark/bin/beeline \
  -u 'jdbc:hive2://127.0.0.1:10000/default' \
  -e "CREATE TABLE IF NOT EXISTS loantrans USING hudi LOCATION 's3a://home-haoyue/hudi/hudi_hybench_sf1333_2.db/loantrans';"
```

## 3) 查询

本机查询：

```bash
/home/ubuntu/disk1/pixels-spark-hudi-thriftserver-0.1-20260416T122600Z/spark/bin/beeline \
  -u 'jdbc:hive2://127.0.0.1:10000/default' \
  -e "select max(freshness_ts) from loantrans;"
```

外部 JDBC URL：

```text
jdbc:hive2://<服务器IP>:10000/default
```

示例 SQL：

```sql
select max(freshness_ts) from loantrans;
```

## 说明

- 这套是本地 metastore 方案，不是 Glue。
- 如果 metastore 被清理，需要重新注册表。
- 外部访问需要放通服务器 `10000/TCP`。
