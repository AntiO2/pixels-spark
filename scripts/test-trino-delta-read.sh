#!/usr/bin/env bash
set -euo pipefail

OPT_ROOT=/home/ubuntu/disk1/opt
TRINO_HOME="$OPT_ROOT/trino-server-466"
TRINO_CLI="$OPT_ROOT/trino-cli/trino"
METASTORE_START="$OPT_ROOT/run/start-metastore.sh"
TEST_ETC=/tmp/trino-readtest-etc
TEST_DATA=/tmp/trino-readtest-data
TEST_LOG=/tmp/trino-readtest.log
TRINO_PID_FILE=/tmp/trino-readtest.pid
SCHEMA=hybench_sf10_readtest
TABLES=(
  customer
  company
  savingAccount
  checkingAccount
  transfer
  checking
  loanapps
  loantrans
)

export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_REGION=us-east-2

cleanup() {
  "$TRINO_HOME/bin/launcher" \
    --etc-dir "$TEST_ETC" \
    --data-dir "$TEST_DATA" \
    --pid-file "$TRINO_PID_FILE" \
    stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup

"$METASTORE_START"
for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 9083 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
nc -z 127.0.0.1 9083 >/dev/null

rm -rf "$TEST_ETC" "$TEST_DATA"
mkdir -p "$TEST_ETC/catalog" "$TEST_DATA"
cp "$TRINO_HOME/etc/jvm.config" "$TEST_ETC/jvm.config"
cp "$TRINO_HOME/etc/log.properties" "$TEST_ETC/log.properties"

cat > "$TEST_ETC/node.properties" <<EOF
node.environment=pixels_local_test
node.id=realtime-pixels-coordinator-readtest
node.data-dir=$TEST_DATA
EOF

cat > "$TEST_ETC/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8081
discovery.uri=http://127.0.0.1:8081
EOF

cat > "$TEST_ETC/catalog/delta_lake.properties" <<EOF
connector.name=delta_lake
hive.metastore.uri=thrift://127.0.0.1:9083
delta.register-table-procedure.enabled=true
delta.enable-non-concurrent-writes=true
fs.native-s3.enabled=true
s3.aws-access-key=$AWS_ACCESS_KEY_ID
s3.aws-secret-key=$AWS_SECRET_ACCESS_KEY
s3.region=us-east-2
s3.endpoint=https://s3.us-east-2.amazonaws.com
EOF

"$TRINO_HOME/bin/launcher" \
  --etc-dir "$TEST_ETC" \
  --data-dir "$TEST_DATA" \
  --pid-file "$TRINO_PID_FILE" \
  --server-log-file "$TEST_LOG" \
  start

for _ in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8081/v1/info >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf http://127.0.0.1:8081/v1/info >/dev/null

for _ in $(seq 1 60); do
  if "$TRINO_CLI" --server http://127.0.0.1:8081 --execute "SHOW CATALOGS" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
"$TRINO_CLI" --server http://127.0.0.1:8081 --execute "SHOW CATALOGS" >/dev/null

"$TRINO_CLI" --server http://127.0.0.1:8081 \
  --execute "CREATE SCHEMA IF NOT EXISTS delta_lake.$SCHEMA"

for table_name in "${TABLES[@]}"; do
  if "$TRINO_CLI" --server http://127.0.0.1:8081 --output-format CSV_HEADER --execute "SELECT count(*) AS present FROM delta_lake.information_schema.tables WHERE table_schema = '$SCHEMA' AND table_name = '$table_name'" | rg -q '"1"'; then
    continue
  fi

  "$TRINO_CLI" --server http://127.0.0.1:8081 \
    --execute "CALL delta_lake.system.register_table(schema_name => '$SCHEMA', table_name => '$table_name', table_location => 's3://home-zinuo/deltalake/hybench_sf10/$table_name')"
done

"$TRINO_CLI" --server http://127.0.0.1:8081 --output-format CSV_HEADER --execute "
SHOW TABLES FROM delta_lake.$SCHEMA;
SELECT count(*) AS customer_count FROM delta_lake.$SCHEMA.customer;
SELECT custid, name FROM delta_lake.$SCHEMA.customer ORDER BY custid LIMIT 3;
"
