#!/bin/bash
# Demonstrates HAProxy failover: generates continuous load, stops one
# backend server mid-test, and confirms HAProxy keeps serving from the
# remaining servers with no downtime. Check the server-distribution chart
# in Metabase/Grafana afterwards - the stopped server should show a gap
# while the others absorb its share of traffic.
#
# Usage: ./failure_test.sh [server_to_kill]  (default: web2)

set -e

TARGET=${1:-web2}
DURATION=60

echo "Generating background load against HAProxy for ${DURATION}s..."
( for _ in $(seq 1 $((DURATION * 10))); do
    curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/
    sleep 0.1
  done > /tmp/failure_test_results.log ) &
LOAD_PID=$!

sleep 15
echo ""
echo ">>> Stopping $TARGET to simulate a server failure..."
docker compose stop "$TARGET"

sleep 20
echo ""
echo ">>> Restarting $TARGET..."
docker compose start "$TARGET"

wait $LOAD_PID

echo ""
echo "Request outcomes during the test (status codes):"
sort /tmp/failure_test_results.log | uniq -c

echo ""
echo "Check HAProxy stats for the outage window: http://localhost:8404/stats"
echo "If HAProxy is working correctly, you should see mostly 200s throughout,"
echo "with $TARGET's health check failing during the outage and its request"
echo "count flatlining while web1/web3 pick up the slack."
