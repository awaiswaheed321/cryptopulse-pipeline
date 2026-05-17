#!/bin/bash

"$(dirname "$0")/venv/bin/python3" - <<'EOF'
from cassandra.cluster import Cluster
from datetime import timezone

session = Cluster(['127.0.0.1'], port=9042).connect()

# ── Row count ────────────────────────────────────────────────────────────────
count = session.execute('SELECT COUNT(*) FROM cryptopulse.real_time_aggregates').one()[0]
print(f"\n{'='*64}")
print(f"  cryptopulse.real_time_aggregates  —  {count} rows total")
print(f"{'='*64}")

# ── Latest 20 rows ───────────────────────────────────────────────────────────
rows = session.execute("""
    SELECT window_start, symbol, average_price, total_volume,
           price_swing_pct, is_anomaly, asset_name, category
    FROM cryptopulse.real_time_aggregates
    LIMIT 20
""")

print(f"\n{'Window Start':<22} {'Symbol':<10} {'Avg Price':>12} {'Volume':>10} {'Swing%':>8} {'Anomaly':<8} {'Asset Name':<20} {'Category'}")
print(f"{'-'*22} {'-'*10} {'-'*12} {'-'*10} {'-'*8} {'-'*8} {'-'*20} {'-'*15}")
for r in rows:
    flag = '🚨 YES' if r.is_anomaly else 'no'
    asset = r.asset_name or 'N/A'
    cat = r.category or 'N/A'
    print(f"{str(r.window_start):<22} {r.symbol:<10} {r.average_price:>12.4f} {r.total_volume:>10.5f} {r.price_swing_pct:>7.3f}% {flag:<8} {asset:<20} {cat}")

# ── Anomalies only ───────────────────────────────────────────────────────────
anomalies = list(session.execute("""
    SELECT window_start, symbol, price_swing_pct, high_price, low_price
    FROM cryptopulse.real_time_aggregates
    WHERE is_anomaly = true
    ALLOW FILTERING
"""))

print(f"\n── Anomalies detected: {len(anomalies)} ──")
for r in anomalies:
    print(f"  {r.symbol}  {str(r.window_start)}  swing={r.price_swing_pct:.3f}%  high={r.high_price}  low={r.low_price}")

print()
EOF
