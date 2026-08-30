#!/bin/bash
# Runs increasing levels of concurrent load against HAProxy and saves the
# results, so you can compare throughput/latency across load levels and
# cross-reference against packet/flow counts for the same time window.
#
# Requires `hey`: https://github.com/rakyll/hey
#
# Usage: ./stress_test.sh

set -e

mkdir -p results

for USERS in 100 500 1000 5000; do
  echo ""
  echo "=== Running with $USERS concurrent users (30s) ==="
  hey -z 30s -c "$USERS" http://localhost:8080/ | tee "results/stress_${USERS}_users.txt"
  echo "Cooling down for 15s before next level..."
  sleep 15
done

echo ""
echo "Done. Results saved under results/stress_<N>_users.txt"
echo "Compare throughput/latency/error-rate across files, and check the"
echo "packets/flows tables (or Metabase) for the matching time windows to"
echo "see how packet rate and retransmissions scale with concurrency."
