#!/bin/bash

echo "=== Stopping pipeline ==="
bash "$(dirname "$0")/stop.sh"

echo ""
echo "=== Clearing Cassandra table ==="
"$(dirname "$0")/venv/bin/python3" -c "
from cassandra.cluster import Cluster
session = Cluster(['127.0.0.1'], port=9042).connect()
session.execute('TRUNCATE cryptopulse.real_time_aggregates')
print('Table truncated.')
"

echo ""
echo "=== Starting fresh ==="
bash "$(dirname "$0")/start_pipeline.sh" --fresh
