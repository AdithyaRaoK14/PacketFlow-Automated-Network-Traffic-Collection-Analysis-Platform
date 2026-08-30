"""
Parses a single .pcap file captured by an Airflow-orchestrated tcpdump run
and loads it into PostgreSQL:

  - packets: one row per packet, with extended metadata (TTL, TCP window,
    retransmission/fragment flags, payload length) plus parsed HTTP fields.
  - flows: packets aggregated into 5-tuple conversations for this capture.
  - captures: updated with packet_count / size_bytes / status.

Designed to be called from the Airflow DAG (packet_pipeline_dag.py), but can
also be run standalone for debugging:

    python parse_packets.py <capture_id> <pcap_path>
"""

import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras
from scapy.all import IP, TCP, UDP, Raw, rdpcap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("parser")

DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "app-db"),
    port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "packets_db"),
    user=os.environ.get("DB_USER", "packets"),
    password=os.environ.get("DB_PASSWORD", "packets"),
)

MAX_DB_RETRIES = 3
DB_RETRY_DELAY_SECONDS = 5

import re

HTTP_REQUEST_RE = re.compile(
    rb"^(GET|POST|PUT|DELETE|HEAD|OPTIONS)\s+(\S+)\s+HTTP/1\.[01]", re.MULTILINE
)
HTTP_STATUS_RE = re.compile(rb"^HTTP/1\.[01]\s+(\d{3})", re.MULTILINE)
HTTP_HOST_RE = re.compile(rb"^Host:\s*(\S+)", re.MULTILINE | re.IGNORECASE)


def get_conn():
    """Connect to Postgres with a few retries, since it may briefly be
    unavailable (restart, resource contention, etc.)."""
    last_exc = None
    for attempt in range(1, MAX_DB_RETRIES + 1):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            log.warning(
                "DB connection attempt %s/%s failed: %s", attempt, MAX_DB_RETRIES, exc
            )
            time.sleep(DB_RETRY_DELAY_SECONDS)
    raise ConnectionError(
        f"Could not connect to PostgreSQL after {MAX_DB_RETRIES} attempts"
    ) from last_exc


def load_server_map(cur):
    """server_name -> ip_address, so packets can be tagged with a friendly
    backend name instead of a raw (and dynamically assigned) container IP."""
    cur.execute("SELECT server_name, ip_address FROM servers")
    ip_to_name = {}
    for server_name, ip_address in cur.fetchall():
        if ip_address:
            ip_to_name[ip_address] = server_name
    return ip_to_name


def parse_pcap_file(filepath, ip_to_server):
    """Returns (packet_rows, flow_rows) for a single pcap file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(filepath)

    try:
        packets = rdpcap(filepath)
    except Exception as exc:  # noqa: BLE001 - corrupted / truncated pcap
        raise ValueError(f"Could not read pcap {filepath}: {exc}") from exc

    seen_tcp_segments = set()  # (src, sport, dst, dport, seq) -> retransmission detection
    flow_acc = defaultdict(
        lambda: {"packet_count": 0, "total_bytes": 0, "start_ts": None, "end_ts": None}
    )
    rows = []

    for pkt in packets:
        if IP not in pkt:
            continue

        ip_layer = pkt[IP]
        pkt_time = datetime.fromtimestamp(float(pkt.time))
        is_fragment = bool(ip_layer.flags.MF) or ip_layer.frag > 0

        row = {
            "ts": pkt_time,
            "src_ip": ip_layer.src,
            "dst_ip": ip_layer.dst,
            "server_name": ip_to_server.get(ip_layer.dst) or ip_to_server.get(ip_layer.src),
            "src_port": None,
            "dst_port": None,
            "protocol": "IP",
            "length": len(pkt),
            "payload_length": 0,
            "ttl": int(ip_layer.ttl),
            "tcp_window": None,
            "tcp_flags": None,
            "is_retransmission": False,
            "is_fragment": is_fragment,
            "http_method": None,
            "http_url": None,
            "http_status": None,
            "http_host": None,
        }

        flow_key = None

        if TCP in pkt:
            tcp_layer = pkt[TCP]
            row["src_port"] = int(tcp_layer.sport)
            row["dst_port"] = int(tcp_layer.dport)
            row["tcp_flags"] = str(tcp_layer.flags)
            row["tcp_window"] = int(tcp_layer.window)
            row["protocol"] = "TCP"

            segment_key = (
                ip_layer.src, tcp_layer.sport, ip_layer.dst, tcp_layer.dport, tcp_layer.seq
            )
            if segment_key in seen_tcp_segments:
                row["is_retransmission"] = True
            else:
                seen_tcp_segments.add(segment_key)

            if Raw in pkt:
                payload = bytes(pkt[Raw].load)
                row["payload_length"] = len(payload)

                req_match = HTTP_REQUEST_RE.search(payload)
                if req_match:
                    row["http_method"] = req_match.group(1).decode(errors="ignore")
                    row["http_url"] = req_match.group(2).decode(errors="ignore")
                    row["protocol"] = "HTTP"

                host_match = HTTP_HOST_RE.search(payload)
                if host_match:
                    row["http_host"] = host_match.group(1).decode(errors="ignore")

                status_match = HTTP_STATUS_RE.search(payload)
                if status_match:
                    row["http_status"] = int(status_match.group(1))
                    row["protocol"] = "HTTP"

            flow_key = (ip_layer.src, ip_layer.dst, tcp_layer.sport, tcp_layer.dport, "TCP")

        elif UDP in pkt:
            udp_layer = pkt[UDP]
            row["src_port"] = int(udp_layer.sport)
            row["dst_port"] = int(udp_layer.dport)
            row["protocol"] = "UDP"
            if Raw in pkt:
                row["payload_length"] = len(bytes(pkt[Raw].load))
            flow_key = (ip_layer.src, ip_layer.dst, udp_layer.sport, udp_layer.dport, "UDP")

        rows.append(row)

        if flow_key:
            acc = flow_acc[flow_key]
            acc["packet_count"] += 1
            acc["total_bytes"] += len(pkt)
            if acc["start_ts"] is None or pkt_time < acc["start_ts"]:
                acc["start_ts"] = pkt_time
            if acc["end_ts"] is None or pkt_time > acc["end_ts"]:
                acc["end_ts"] = pkt_time

    flow_rows = []
    for (src_ip, dst_ip, src_port, dst_port, protocol), acc in flow_acc.items():
        duration = (acc["end_ts"] - acc["start_ts"]).total_seconds()
        flow_rows.append(
            {
                "src_ip": src_ip,
                "dst_ip": dst_ip,
                "src_port": src_port,
                "dst_port": dst_port,
                "protocol": protocol,
                "packet_count": acc["packet_count"],
                "total_bytes": acc["total_bytes"],
                "start_ts": acc["start_ts"],
                "end_ts": acc["end_ts"],
                "duration_seconds": duration,
            }
        )

    return rows, flow_rows


def insert_packets(cur, capture_id, rows):
    if not rows:
        return
    columns = [
        "capture_id", "ts", "src_ip", "dst_ip", "server_name", "src_port", "dst_port",
        "protocol", "length", "payload_length", "ttl", "tcp_window", "tcp_flags",
        "is_retransmission", "is_fragment", "http_method", "http_url", "http_status",
        "http_host",
    ]
    values = [
        tuple([capture_id] + [r[c] for c in columns if c != "capture_id"]) for r in rows
    ]
    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO packets ({', '.join(columns)}) VALUES %s",
        values,
        page_size=1000,
    )


def insert_flows(cur, capture_id, flow_rows):
    if not flow_rows:
        return
    columns = [
        "capture_id", "src_ip", "dst_ip", "src_port", "dst_port", "protocol",
        "packet_count", "total_bytes", "start_ts", "end_ts", "duration_seconds",
    ]
    values = [
        tuple([capture_id] + [r[c] for c in columns if c != "capture_id"]) for r in flow_rows
    ]
    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO flows ({', '.join(columns)}) VALUES %s",
        values,
        page_size=1000,
    )


def parse_and_store(capture_id, pcap_path):
    """Main entry point called by the Airflow DAG for a single capture."""
    started = time.time()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        ip_to_server = load_server_map(cur)

        try:
            rows, flow_rows = parse_pcap_file(pcap_path, ip_to_server)
        except (FileNotFoundError, ValueError) as exc:
            log.error("Failed to parse %s: %s", pcap_path, exc)
            cur.execute(
                "UPDATE captures SET status = 'error', error_message = %s WHERE id = %s",
                (str(exc), capture_id),
            )
            conn.commit()
            return

        insert_packets(cur, capture_id, rows)
        insert_flows(cur, capture_id, flow_rows)

        size_bytes = os.path.getsize(pcap_path) if os.path.exists(pcap_path) else 0
        cur.execute(
            """
            UPDATE captures
            SET packet_count = %s, size_bytes = %s, status = 'parsed'
            WHERE id = %s
            """,
            (len(rows), size_bytes, capture_id),
        )
        conn.commit()

        http_count = sum(1 for r in rows if r["http_method"])
        tcp_count = sum(1 for r in rows if r["protocol"] in ("TCP", "HTTP"))
        retrans_count = sum(1 for r in rows if r["is_retransmission"])
        elapsed = time.time() - started
        log.info(
            "Capture %s: found %d packets (HTTP=%d, TCP=%d, retransmissions=%d), "
            "%d flows, inserted in %.2fs",
            capture_id, len(rows), http_count, tcp_count, retrans_count,
            len(flow_rows), elapsed,
        )
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        log.exception("Unexpected error parsing capture %s", capture_id)
        try:
            cur.execute(
                "UPDATE captures SET status = 'error', error_message = %s WHERE id = %s",
                (str(exc), capture_id),
            )
            conn.commit()
        except Exception:  # noqa: BLE001
            log.exception("Could not even record the error state for capture %s", capture_id)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python parse_packets.py <capture_id> <pcap_path>")
        sys.exit(1)
    parse_and_store(int(sys.argv[1]), sys.argv[2])
