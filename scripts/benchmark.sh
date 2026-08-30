#!/bin/bash
# Reports real numbers instead of "fast": packets/sec captured, parsing
# throughput, and how many captures/packets/flows have accumulated so far.
# Run this after a handful of DAG runs have completed (Phase 4 in the
# README), ideally under different loads via scripts/stress_test.sh, so you
# have something to compare rather than a single snapshot.
#
# Usage: ./benchmark.sh

set -e

echo "=== Capture-level benchmarks (from captures table) ==="
docker exec -i app-db psql -U packets -d packets_db -t -A -F' | ' -c "
SELECT
    capture_label,
    balance_algorithm,
    packet_count,
    size_bytes,
    EXTRACT(EPOCH FROM (end_time - start_time)) AS wall_seconds,
    ROUND(packet_count / NULLIF(EXTRACT(EPOCH FROM (end_time - start_time)), 0), 1) AS packets_per_sec
FROM captures
WHERE status = 'archived'
ORDER BY id DESC
LIMIT 10;
"

echo ""
echo "=== Overall totals ==="
docker exec -i app-db psql -U packets -d packets_db -t -A -F' | ' -c "
SELECT
    (SELECT COUNT(*) FROM captures)  AS total_captures,
    (SELECT COUNT(*) FROM packets)   AS total_packets,
    (SELECT COUNT(*) FROM flows)     AS total_flows,
    (SELECT pg_size_pretty(pg_total_relation_size('packets'))) AS packets_table_size;
"

echo ""
echo "=== Parsing/insert throughput (from Airflow scheduler logs) ==="
echo "Look for lines like:"
echo '  \"Capture N: found X packets ... inserted in Y.YYs\"'
echo "in the scheduler logs - that Y value already reports parse+insert time"
echo "for exactly X packets, giving you packets/sec end to end:"
docker compose logs airflow-scheduler 2>/dev/null | grep "inserted in" | tail -10 || \
  echo "(no matching log lines yet - let a few more DAG runs complete)"

echo ""
echo "=== Dashboard refresh time ==="
echo "Metabase: open a question, check the query time shown at the bottom"
echo "of the results table (e.g. 'Showing 200 rows in 0.4s')."
echo "Grafana: panels default to a 10s scrape interval (see"
echo "monitoring/prometheus/prometheus.yml scrape_interval) - lower it if"
echo "you need to report near-real-time refresh behavior."
