"""
packet_capture_pipeline
========================

Airflow owns the entire lifecycle of a capture run, instead of tcpdump
running forever inside the haproxy container:

    resolve_servers -> create_capture -> start_capture -> wait_for_capture
        -> parse_and_store -> archive_capture -> cleanup_old_archives

Each run produces one row in `captures`, tagged with a label like
`capture_20260713_150000`, so every pcap is versioned and traceable back to
exactly which DAG run produced it.

Requires:
  - /var/run/docker.sock mounted into the Airflow containers (to start/stop
    tcpdump inside the `haproxy` container without SSH).
  - the `docker`, `scapy`, and `psycopg2-binary` Python packages (installed
    via _PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yml).
  - /opt/airflow/pcap to be the SAME shared volume mounted at /pcap inside
    the haproxy container.
"""

import gzip
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta

import docker
import psycopg2

sys.path.append("/opt/airflow/parser")

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

PCAP_DIR = os.environ.get("PCAP_DIR", "/opt/airflow/pcap")
ARCHIVE_DIR = os.path.join(PCAP_DIR, "archive")
HAPROXY_CONTAINER = "haproxy"
BACKEND_SERVERS = ["web1", "web2", "web3"]

DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "app-db"),
    port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "packets_db"),
    user=os.environ.get("DB_USER", "packets"),
    password=os.environ.get("DB_PASSWORD", "packets"),
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _capture_duration():
    return int(Variable.get("CAPTURE_DURATION_SECONDS", default_var=55))


def _archive_retention_days():
    return int(Variable.get("ARCHIVE_RETENTION_DAYS", default_var=3))


def _balance_algorithm():
    # Set this Airflow Variable manually when you swap haproxy.cfg to a
    # different balancing algorithm, so captures/flows stay labeled
    # correctly for comparison (see scripts/ + README "Comparing load
    # balancing algorithms").
    return Variable.get("BALANCE_ALGORITHM", default_var="roundrobin")


def _get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------
# Task 1: resolve backend container IPs so packets/flows can be tagged
# with a friendly server_name instead of a raw, dynamically-assigned IP
# ---------------------------------------------------------------------
def resolve_servers(**context):
    client = docker.from_env()
    conn = _get_conn()
    cur = conn.cursor()
    try:
        resolved = 0
        for name in BACKEND_SERVERS:
            try:
                container = client.containers.get(name)
            except docker.errors.NotFound:
                log.warning("Container %s not found, skipping", name)
                continue

            networks = container.attrs["NetworkSettings"]["Networks"]
            ip_address = None
            for net_info in networks.values():
                if net_info.get("IPAddress"):
                    ip_address = net_info["IPAddress"]
                    break

            if not ip_address:
                log.warning("Could not resolve IP for %s", name)
                continue

            cur.execute(
                """
                INSERT INTO servers (server_name, ip_address, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (server_name)
                DO UPDATE SET ip_address = EXCLUDED.ip_address, updated_at = now()
                """,
                (name, ip_address),
            )
            resolved += 1

        conn.commit()
        log.info("Resolved %d/%d backend server IPs", resolved, len(BACKEND_SERVERS))
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------
# Task 2: create a versioned capture record and push its id/filename
# ---------------------------------------------------------------------
def create_capture(**context):
    label = f"capture_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    filename = f"{label}.pcap"

    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO captures (capture_label, balance_algorithm, pcap_file, status)
            VALUES (%s, %s, %s, 'pending')
            RETURNING id
            """,
            (label, _balance_algorithm(), filename),
        )
        capture_id = cur.fetchone()[0]
        conn.commit()
    finally:
        cur.close()
        conn.close()

    context["ti"].xcom_push(key="capture_id", value=capture_id)
    context["ti"].xcom_push(key="filename", value=filename)
    log.info("Created capture record id=%s label=%s", capture_id, label)


# ---------------------------------------------------------------------
# Task 3: start a self-terminating tcpdump inside the haproxy container
# ---------------------------------------------------------------------
def start_capture(**context):
    ti = context["ti"]
    capture_id = ti.xcom_pull(key="capture_id", task_ids="create_capture")
    filename = ti.xcom_pull(key="filename", task_ids="create_capture")
    duration = _capture_duration()

    client = docker.from_env()
    container = client.containers.get(HAPROXY_CONTAINER)

    # `timeout` makes tcpdump exit on its own after `duration` seconds, so
    # there's no separate "stop capture" call needed - the wait task just
    # has to sleep long enough for it to finish and flush to disk.
    cmd = f"sh -c 'timeout {duration} tcpdump -i eth0 -w /pcap/{filename}'"
    container.exec_run(cmd, detach=True)

    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE captures SET status = 'capturing', start_time = now() WHERE id = %s",
            (capture_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    log.info(
        "Started tcpdump in %s for %ss -> %s (capture_id=%s)",
        HAPROXY_CONTAINER, duration, filename, capture_id,
    )


# ---------------------------------------------------------------------
# Task 4: wait for capture + a safety margin for tcpdump to flush to disk
# ---------------------------------------------------------------------
def wait_for_capture(**context):
    duration = _capture_duration()
    margin = 10
    log.info("Waiting %ss for capture to finish...", duration + margin)
    time.sleep(duration + margin)


# ---------------------------------------------------------------------
# Task 5: parse the finished pcap and load packets + flows into Postgres
# ---------------------------------------------------------------------
def parse_and_store(**context):
    import parse_packets  # local module, mounted at /opt/airflow/parser

    ti = context["ti"]
    capture_id = ti.xcom_pull(key="capture_id", task_ids="create_capture")
    filename = ti.xcom_pull(key="filename", task_ids="create_capture")
    pcap_path = os.path.join(PCAP_DIR, filename)

    parse_packets.parse_and_store(capture_id, pcap_path)


# ---------------------------------------------------------------------
# Task 6: compress the raw pcap and move it to the archive folder
# ---------------------------------------------------------------------
def archive_capture(**context):
    ti = context["ti"]
    capture_id = ti.xcom_pull(key="capture_id", task_ids="create_capture")

    conn = _get_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE captures
            SET status='archived',
                end_time=now()
            WHERE id=%s
            """,
            (capture_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    log.info("Archive step skipped.")


# ---------------------------------------------------------------------
# Task 7: delete archives past their retention window so disk usage
# doesn't grow forever
# ---------------------------------------------------------------------
def cleanup_old_archives(**context):
    if not os.path.isdir(ARCHIVE_DIR):
        return

    retention_days = _archive_retention_days()
    cutoff = time.time() - retention_days * 86400
    removed = 0

    for fname in os.listdir(ARCHIVE_DIR):
        fpath = os.path.join(ARCHIVE_DIR, fname)
        if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
            os.remove(fpath)
            removed += 1

    log.info("Cleanup: removed %d archive(s) older than %d day(s)", removed, retention_days)


with DAG(
    dag_id="packet_capture_pipeline",
    description=(
        "Airflow-orchestrated capture: start tcpdump, wait, parse into "
        "packets/flows, archive the pcap, then clean up old archives."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["networking", "pcap", "etl"],
) as dag:

    t1 = PythonOperator(task_id="resolve_servers", python_callable=resolve_servers)
    t2 = PythonOperator(task_id="create_capture", python_callable=create_capture)
    t3 = PythonOperator(task_id="start_capture", python_callable=start_capture)
    t4 = PythonOperator(task_id="wait_for_capture", python_callable=wait_for_capture)
    t5 = PythonOperator(task_id="parse_and_store", python_callable=parse_and_store)
    t6 = PythonOperator(task_id="archive_capture", python_callable=archive_capture)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
