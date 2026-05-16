#!/bin/bash

# Pass --fresh to wipe Spark checkpoints and force a cold start.
# Without this flag, the pipeline resumes from the last committed Kafka offset.
FRESH_START=false
for arg in "$@"; do
    [[ "$arg" == "--fresh" ]] && FRESH_START=true
done

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

echo "Waiting for Cassandra to be ready..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' cassandra 2>/dev/null)" = "healthy" ]; do
    echo "  Cassandra not ready yet, retrying in 5s..."
    sleep 5
done
echo "Cassandra is ready."

echo "Initializing Cassandra Schema..."
docker exec -i cassandra cqlsh < src/cassandra/schema.cql

echo "Uploading metadata CSV to HDFS..."
docker cp data/crypto_metadata.csv namenode:/tmp/crypto_metadata.csv
docker exec namenode sh -c "hdfs dfs -mkdir -p /user/data && hdfs dfs -put -f /tmp/crypto_metadata.csv /user/data/crypto_metadata.csv && hdfs dfs -setrep -w 1 /user/data/crypto_metadata.csv"
echo "HDFS upload complete and block replicated."

echo "Resolving HDFS namenode IP for host-side Spark..."
NAMENODE_IP=$(docker inspect namenode --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null)
DATANODE_IP=$(docker inspect datanode --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null)
export HDFS_NAMENODE="${NAMENODE_IP}:9000"
echo "HDFS_NAMENODE set to $HDFS_NAMENODE"
echo "DATANODE_IP set to $DATANODE_IP"

if [ "$FRESH_START" = true ]; then
    echo "Clearing Spark checkpoints for fresh start..."
    rm -rf /tmp/spark_checkpoints_crypto
else
    echo "Resuming from existing Spark checkpoints (run with --fresh to start clean)."
fi

echo "Starting Binance Producer..."
python -m src.ingestion.binance_producer &
PRODUCER_PID=$!

echo "Starting Spark Streaming Engine..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.cassandra.connection.host=127.0.0.1 \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  --conf spark.hadoop.dfs.replication=1 \
  --conf "spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy=NEVER" \
  src/processing/spark_streaming.py &
SPARK_PID=$!

echo "Pipeline is running in the background!"
echo "Press [Ctrl+C] to safely shut down both processes."

trap "echo 'Shutting down pipeline...'; kill -9 $PRODUCER_PID $SPARK_PID 2>/dev/null; exit 0" INT
wait