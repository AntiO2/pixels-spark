export OPT_ROOT=/home/ubuntu/disk1/opt
export SPARK_HOME=$OPT_ROOT/spark-3.5.6-bin-hadoop3
export METASTORE_HOME=$OPT_ROOT/apache-hive-metastore-3.1.2-bin
export TRINO_HOME=$OPT_ROOT/trino-server-466
export TRINO_CLI=$OPT_ROOT/trino-cli/trino
export JAVA17_HOME=$OPT_ROOT/jdk-17.0.14+7
export JAVA11_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export JAVA23_HOME=/usr/lib/jvm/zulu-23-amd64
export METASTORE_HOST=realtime-pixels-coordinator
export METASTORE_PORT=9083
export METASTORE_URI=thrift://realtime-pixels-coordinator:9083
export METASTORE_DB_DIR=$OPT_ROOT/metastore-db
export HIVE_WAREHOUSE_DIR=$OPT_ROOT/hive-warehouse
export DELTA_ROOT=$OPT_ROOT/pixels-delta
export PATH=$SPARK_HOME/bin:$TRINO_HOME/bin:$PATH

export HADOOP_HOME=$OPT_ROOT/hadoop-stub
export PATH=$HADOOP_HOME/bin:$PATH
