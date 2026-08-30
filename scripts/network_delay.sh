#!/bin/bash
# Injects artificial latency and packet loss on a container's network
# interface using `tc netem`, so you can observe how HAProxy/Apache behave
# under degraded network conditions.
#
# Usage:
#   ./network_delay.sh <container_name> <delay> <loss_percent>
#   ./network_delay.sh web2 200ms 5%
#
# To remove the impairment again:
#   docker exec <container_name> tc qdisc del dev eth0 root netem

set -e

CONTAINER=${1:-web2}
DELAY=${2:-200ms}
LOSS=${3:-5%}

echo "Installing iproute2 (for tc) in $CONTAINER if not already present..."
docker exec "$CONTAINER" sh -c \
  "which tc >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq iproute2 >/dev/null 2>&1)"

echo "Adding ${DELAY} delay and ${LOSS} packet loss to ${CONTAINER}'s eth0..."
docker exec "$CONTAINER" tc qdisc add dev eth0 root netem delay "$DELAY" loss "$LOSS"

echo "Done. Generate some traffic now and watch latency/timeouts increase."
echo "Remove it again with:"
echo "  docker exec $CONTAINER tc qdisc del dev eth0 root netem"
