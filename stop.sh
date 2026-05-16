#!/bin/bash

echo "Stopping pipeline processes..."
pkill -TERM -f binance_producer 2>/dev/null || true
pkill -TERM -f SparkSubmit      2>/dev/null || true
pkill -TERM -f spark_streaming  2>/dev/null || true

sleep 3

# Force kill anything that didn't exit cleanly
pkill -9 -f binance_producer 2>/dev/null || true
pkill -9 -f SparkSubmit      2>/dev/null || true
pkill -9 -f spark_streaming  2>/dev/null || true

echo "Pipeline stopped."
