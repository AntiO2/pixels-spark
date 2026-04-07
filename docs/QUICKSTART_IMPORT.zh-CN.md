# Delta Lake 导入快速指南

本文档只保留最短路径：

1. 启动 HMS 和 Trino
2. 重导 `sf10` 到 S3
3. 在 Trino 中重新注册表
4. 用 Trino CLI 查询验证

## 1. 启动基础服务

启动 HMS：

```bash
/home/ubuntu/disk1/opt/run/start-metastore.sh
```

启动 Trino：

```bash
/home/ubuntu/disk1/opt/run/start-trino.sh
```

如果 Trino 要查询 S3 上的 Delta 表，先确认：

- `/home/ubuntu/disk1/opt/trino-server-466/etc/catalog/delta_lake.properties`

至少包含：

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

模板文件在：

- [etc/trino-delta_lake.properties.example](../etc/trino-delta_lake.properties.example)

## 2. 重导 `sf10` 到 S3

项目配置文件：

- [etc/pixels-spark.properties](../etc/pixels-spark.properties)

确认 bucket 数配置：

```properties
pixels.spark.delta.hash-bucket.count=16
```

执行整库导入：

```bash
./scripts/run-import-hybench-sf10.sh \
  /home/ubuntu/disk1/hybench_sf10 \
  s3a://home-zinuo/deltalake/hybench_sf10
```

说明：

- 导入是 `overwrite`
- 导入时会写入持久列 `_pixels_bucket_id`
- 计算方式为 `pmod(hash(pk), x)`

## 3. 在 Trino 中重新注册表

重导或改 partition 后，重新注册：

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

## 4. 查询验证

查看表：

```bash
/home/ubuntu/disk1/opt/trino-cli/trino \
  --server http://127.0.0.1:8080 \
  --execute "SHOW TABLES FROM delta_lake.hybench_sf10"
```

查一条数据：

```bash
/home/ubuntu/disk1/opt/trino-cli/trino \
  --server http://127.0.0.1:8080 \
  --execute "SELECT * FROM delta_lake.hybench_sf10.customer LIMIT 1"
```

## 5. 常见问题

`SHOW TABLES` 能看到表，但 `SELECT` 失败：

- 先检查 `delta_lake.properties` 里是否真的加了 S3 配置
- 确认 Trino 已经重启并加载了新配置

如果报：

```text
No factory for location: s3://.../_delta_log
```

说明当前 Trino 还不会读 S3。

如果报：

```text
Error getting snapshot for hybench_sf10.customer
```

通常也是 Trino 侧的 S3 / Delta 读取配置还没生效。
