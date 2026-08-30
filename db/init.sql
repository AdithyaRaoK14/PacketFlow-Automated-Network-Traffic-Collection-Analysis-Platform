-- ============================================================
-- captures: one row per Airflow-orchestrated capture run
-- ============================================================
CREATE TABLE IF NOT EXISTS captures (
    id                 SERIAL PRIMARY KEY,
    capture_label      TEXT UNIQUE NOT NULL,       -- e.g. capture_20260713_150000
    balance_algorithm  TEXT DEFAULT 'roundrobin',   -- which haproxy.cfg was active
    pcap_file          TEXT,                        -- filename inside the pcap volume
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    packet_count       INTEGER DEFAULT 0,
    size_bytes         BIGINT DEFAULT 0,
    status             TEXT DEFAULT 'pending',       -- pending -> capturing -> parsing -> parsed -> archived -> error
    error_message      TEXT
);

-- ============================================================
-- servers: resolved container-name -> IP mapping, refreshed each
-- DAG run so packets/flows can be joined to a human-readable name
-- even though Docker assigns IPs dynamically.
-- ============================================================
CREATE TABLE IF NOT EXISTS servers (
    server_name  TEXT PRIMARY KEY,   -- web1 / web2 / web3
    ip_address   TEXT,
    updated_at   TIMESTAMP DEFAULT now()
);

-- ============================================================
-- packets: one row per captured packet, enriched with metadata
-- beyond the basics (TTL, TCP window, retransmission/fragment
-- flags, payload length) plus parsed HTTP fields when present.
-- ============================================================
CREATE TABLE IF NOT EXISTS packets (
    id                  SERIAL PRIMARY KEY,
    capture_id          INTEGER REFERENCES captures(id) ON DELETE CASCADE,
    ts                  TIMESTAMP,
    src_ip              TEXT,
    dst_ip              TEXT,
    server_name         TEXT,               -- resolved via servers table (NULL if not a backend)
    src_port            INTEGER,
    dst_port            INTEGER,
    protocol            TEXT,
    length              INTEGER,
    payload_length      INTEGER,
    ttl                 INTEGER,
    tcp_window          INTEGER,
    tcp_flags           TEXT,
    is_retransmission   BOOLEAN DEFAULT FALSE,
    is_fragment         BOOLEAN DEFAULT FALSE,
    http_method         TEXT,
    http_url            TEXT,
    http_status         INTEGER,
    http_host           TEXT
);

CREATE INDEX IF NOT EXISTS idx_packets_capture_id ON packets (capture_id);
CREATE INDEX IF NOT EXISTS idx_packets_ts          ON packets (ts);
CREATE INDEX IF NOT EXISTS idx_packets_dst_ip       ON packets (dst_ip);
CREATE INDEX IF NOT EXISTS idx_packets_server_name  ON packets (server_name);

-- ============================================================
-- flows: packets aggregated into 5-tuple conversations per
-- capture ("client -> server: N packets, M bytes, T seconds")
-- ============================================================
CREATE TABLE IF NOT EXISTS flows (
    id                 SERIAL PRIMARY KEY,
    capture_id         INTEGER REFERENCES captures(id) ON DELETE CASCADE,
    src_ip             TEXT,
    dst_ip             TEXT,
    src_port           INTEGER,
    dst_port           INTEGER,
    protocol           TEXT,
    packet_count       INTEGER,
    total_bytes        BIGINT,
    start_ts           TIMESTAMP,
    end_ts             TIMESTAMP,
    duration_seconds   NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_flows_capture_id ON flows (capture_id);
CREATE INDEX IF NOT EXISTS idx_flows_dst_ip      ON flows (dst_ip);
