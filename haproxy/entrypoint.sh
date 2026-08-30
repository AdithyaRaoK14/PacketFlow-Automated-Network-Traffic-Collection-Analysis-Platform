#!/bin/bash
set -e

mkdir -p /pcap

# NOTE: tcpdump is intentionally NOT auto-started here anymore. Airflow now
# owns the capture lifecycle end-to-end (start -> wait -> stop -> parse ->
# archive) via `docker exec haproxy tcpdump ...`, using the Docker socket
# mounted into the Airflow containers. See airflow/dags/packet_pipeline_dag.py.
#
# tcpdump is still installed in this image (see Dockerfile) so those execs
# work, and NET_ADMIN/NET_RAW are still granted in docker-compose.yml.

echo "[entrypoint] haproxy starting (tcpdump is orchestrated by Airflow, not auto-started)"

# Run HAProxy in the foreground so the container stays alive / logs show up
exec haproxy -f /usr/local/etc/haproxy/haproxy.cfg
