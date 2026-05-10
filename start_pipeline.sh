#!/bin/bash

echo "Detecting Java home for Spark compatibility..."
if [[ "$(uname)" == "Darwin" ]]; then
    export JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null)"
else
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"
fi

echo "Setting Python Path to root directory..."
export PYTHONPATH=$(pwd)

echo "Activating Virtual Environment..."
source venv/bin/activate
export SPARK_HOME="$(python3 -c 'import pyspark; import os; print(os.path.dirname(pyspark.__file__))')"
export PATH="$SPARK_HOME/bin:$PATH"

echo "Initializing Kafka Topic..."
docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic crypto_trades \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Initializing Cassandra Schema..."
docker exec -i cassandra cqlsh < src/cassandra/schema.cql

echo "Uploading metadata CSV to HDFS..."
docker cp data/crypto_metadata.csv namenode:/tmp/crypto_metadata.csv
docker exec namenode hdfs dfs -mkdir -p /user/data
docker exec namenode hdfs dfs -put -f /tmp/crypto_metadata.csv /user/data/crypto_metadata.csv

echo "Resolving HDFS namenode IP for host-side Spark..."
NAMENODE_IP=$(docker inspect namenode --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null)
export HDFS_NAMENODE="${NAMENODE_IP}:9000"
echo "HDFS_NAMENODE set to $HDFS_NAMENODE"

echo "Clearing stale Spark checkpoints..."
rm -rf /tmp/spark_checkpoints_crypto

echo "Starting Binance Producer..."
python -m src.ingestion.binance_producer &
PRODUCER_PID=$!

echo "Starting Spark Streaming Engine..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.cassandra.connection.host=127.0.0.1 \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  src/processing/spark_streaming.py &
SPARK_PID=$!

echo "Pipeline is running in the background!"
echo "Press [Ctrl+C] to safely shut down both processes."

trap "echo '🛑 Shutting down pipeline...'; kill -9 $PRODUCER_PID $SPARK_PID 2>/dev/null; exit 0" INT
wait