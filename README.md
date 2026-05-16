# Real-Time Multi-Asset Crypto Pipeline & Analytics

## Distributed Big Data Architecture Overview

This project implements a production-grade, end-to-end Big Data pipeline designed to ingest, process, enrich, and visualize high-frequency cryptocurrency trade data in real time. The architecture leverages a containerized environment to orchestrate multiple distributed systems, ensuring fault tolerance, seamless service discovery via Docker bridge networks, and scalability.

---

## Core Features

### Real-Time Data Ingestion (Apache Kafka)

- **Data Source:** Connects to the Binance.US WebSocket API (`wss://stream.binance.us:9443`).
- **Combined Streams:** Multiplexes connections to ingest live trade data for various crypto pairs (BTC, ETH, SOL, XRP, BNB, ADA) simultaneously through a single persistent connection to reduce network overhead.
- **Producer Logic:** A robust Python script utilizing the `kafka-python` library for broker communication and `websocket-client` for real-time streaming.
- **Message Delivery:** Asynchronously publishes raw JSON trade data into an Apache Kafka topic named `crypto_trades`.

### Distributed Processing (Spark Structured Streaming)

- **Engine:** Utilizes Apache Spark 3.x with Structured Streaming to subscribe to the Kafka topic.
- **Real-Time Transformations:** Calculates meaningful aggregations, specifically the `average_price` and `total_volume` over 1-minute tumbling windows.
- **Adapter Integration:** The `spark-submit` command passes a comma-separated list to the `--packages` flag to dynamically provision both the Kafka adapter and the Cassandra connector.
- **State Management:** Implements event-time watermarking to handle late-arriving data and prevent state memory bloat.

### Real-Time Anomaly Detection (Volatility Tracking)

- **Streaming-Safe Math:** Calculates the high/low price spread natively within Spark's tumbling windows to avoid the memory overhead of non-time-based windowing functions.
- **Statistical Flagging:** Computes the `price_swing_pct` for every asset, every minute. If an asset's price fluctuates by more than 1.5% within a single 60-second window, the pipeline triggers an `is_anomaly = True` boolean flag.
- **Optimized Storage:** Stores anomalies as highly-indexable booleans rather than text strings, enabling rapid NoSQL querying and live "Alert Feeds" in the visualization layer.

### Data Enrichment with Spark SQL (HDFS Integration)

- **Static Reference Data:** A static CSV file containing asset metadata (e.g., Full Name, Category, Market Type) is stored within the Hadoop Distributed File System (HDFS) at `/user/data/crypto_metadata.csv`.
- **Stream-Static Join:** The Spark application performs a live join between the streaming Kafka DataFrame and the static HDFS dataset using Spark SQL (`createOrReplaceTempView` + `spark.sql()`).
- **Outcome:** Raw transaction symbols (e.g., `BTCUSDT`) are enriched with human-readable context (e.g., `Bitcoin`, `Layer 1`) before being persisted.

### Persistent Storage (Apache Cassandra)

- **Sink Integration:** Processed DataFrames are written to a persistent NoSQL storage layer optimized for time-series data using Spark's `foreachBatch` logic.
- **Schema Management:** Includes a `cassandra/schema.cql` file for automated keyspace (`cryptopulse`) and table (`real_time_aggregates`) initialization.
- **Docker Volumes:** Data is persisted on the local host machine, ensuring database records survive container restarts and pipeline shutdowns.

### Visualization & Dashboarding (Grafana)

- **Auto-Provisioned:** Grafana datasource and dashboards are automatically loaded on startup via provisioning files in `grafana/provisioning/` and `grafana/dashboards/` — no manual setup required.
- **Live Dashboard:** Connects Cassandra to Grafana using the `hadesarchitect-cassandra-datasource` plugin with a 30-second auto-refresh.
- **Logarithmic Scaling:** Employs base-10 logarithmic scaling to accurately compare high-value assets (Bitcoin) against lower-value assets (ADA/XRP) on the same Y-axis.
- **Advanced Data Transformations:** Uses a transformation pipeline consisting of `Merge series`, `Sort by (time)`, and `Partition by values` to format raw NoSQL multi-frame data into clean, continuous time-series visual lines.

### Full Observability Stack (Prometheus)

- **Health Monitoring:** Integrates Prometheus to scrape metrics and monitor infrastructure health.
- **Dynamic Configuration:** Uses Docker bind mounts for `prometheus.yml`, allowing real-time configuration updates without container rebuilds.

### Infrastructure Automation & Orchestration

- **Docker Networking:** Enforces network isolation and automatic service discovery by placing all containers (Zookeeper, Kafka, Cassandra, Hadoop, Prometheus, Grafana) on a dedicated `big-data-net` bridge network.
- **Idempotent Initialization:** The custom `start_pipeline.sh` orchestrator script utilizes `--if-not-exists` flags and the `schema.cql` file to automatically provision Kafka topics and Cassandra tables on startup without manual intervention or crashing.
- **Graceful Shutdowns:** Traps `SIGINT` (Ctrl+C) signals to simultaneously and safely terminate both the Python producer and the Spark Streaming engine.

---

## Prerequisites

The following must be installed on your machine before running the pipeline:

- **Docker** (with the `docker compose` plugin) — to run all infrastructure services
- **Java 17+** — required by Apache Spark (`sudo apt install openjdk-17-jdk` on Ubuntu)
- **Python 3.10+** with `venv` support (`sudo apt install python3-venv` on Ubuntu if not present)

---

## Configuration (.env)

The project reads runtime settings from a `.env` file at the repository root. Copy the example file and edit if needed:

```bash
cp .env.example .env
```

Default values work out of the box:

```text
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=crypto_trades
BINANCE_WS_URL=wss://stream.binance.us:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade/bnbusdt@trade/adausdt@trade/xrpusdt@trade
GRAFANA_ADMIN_PASSWORD=admin
```

---

## How to Run the Pipeline

**1. Start the Infrastructure**

Launch the containerized environment and wait for all services to become healthy:

```bash
docker compose up -d
```

**2. Prepare the Python Environment**

Create a virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**3. Execute the Pipeline Orchestrator**

Run the custom Bash script. It handles everything automatically: detects your Java installation, waits for Cassandra to be ready, initializes the Cassandra schema, creates the Kafka topic, uploads the static metadata CSV to HDFS, and launches both the Binance producer and the Spark Streaming engine:

```bash
chmod +x start_pipeline.sh
./start_pipeline.sh
```

The script resumes from the last committed Kafka offset by default. Pass `--fresh` to wipe Spark checkpoints and start from zero (recommended for demos):

```bash
./start_pipeline.sh --fresh
```

**4. View the Dashboard**

Navigate to http://localhost:3000

Login with `admin` / `admin`

The **Crypto-Stream Dashboard** loads automatically and begins displaying live data within 1–2 minutes of pipeline startup.

---

## Troubleshooting & Operations

**Spark ↔ Kafka Connectivity:** When running Spark locally on the host, use `localhost:9092`. If containerizing the Spark job, use the Docker bridge network DNS: `kafka:29092`.

**Verify Database Records:**

```bash
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM cryptopulse.real_time_aggregates;"
```

**Clear Spark Checkpoints (force a cold start):**

```bash
./start_pipeline.sh --fresh
```

Checkpoints are preserved across normal restarts so the pipeline resumes from where it left off. Use `--fresh` only when you want to start from zero (e.g., after a Kafka topic reset or for a clean demo).

**Safely Stop the Infrastructure (data remains saved in volumes):**

```bash
docker compose stop
```

**Full Reset (stop containers, wipe Cassandra data, fresh start):**

```bash
docker compose down
# Then truncate the table after bringing services back up:
docker exec cassandra cqlsh -e "TRUNCATE cryptopulse.real_time_aggregates;"
```
