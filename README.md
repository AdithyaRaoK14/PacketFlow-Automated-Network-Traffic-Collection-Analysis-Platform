# PacketFlow

### Automated Network Traffic Collection & Analysis Platform

[![CI](https://github.com/AdithyaRaoK14/packetflow/actions/workflows/ci.yml/badge.svg)](https://github.com/AdithyaRaoK14/packetflow/actions/workflows/ci.yml)
[![Docker Build](https://github.com/AdithyaRaoK14/packetflow/actions/workflows/docker-build.yml/badge.svg)](https://github.com/AdithyaRaoK14/packetflow/actions/workflows/docker-build.yml)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Storage-PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)

An M.E.-level systems project: a Dockerized mini production environment that
generates traffic, load-balances it, captures and parses packets under full
Airflow orchestration, and exposes both infrastructure metrics and traffic
analytics.

> **One-line description you can hand a professor:**
> An automated network traffic collection and analysis platform that
> simulates a production environment using Docker. The system generates
> traffic with JMeter, distributes it through HAProxy to multiple Apache
> servers, captures network packets using tcpdump orchestrated end-to-end by
> Apache Airflow, parses and stores structured packet/flow metadata in
> PostgreSQL, and visualizes traffic analytics and infrastructure health
> through Metabase and Grafana. It also supports multiple load-balancing
> strategies, server failure simulations, and performance benchmarking under
> varying traffic loads.

## Table of contents
- [Architecture](#architecture)
- [What you'll learn](#what-youll-learn)
- [Thesis / defense prep](#thesis--defense-prep)
- [Prerequisites](#prerequisites)
- [Project layout](#project-layout)
- [Getting started](#phase-1--start-everything)
- [Comparing load balancing algorithms](#comparing-load-balancing-algorithms)
- [Resilience & performance experiments](#resilience--performance-experiments-tier-3)
- [Troubleshooting](#troubleshooting)
- [Extending further](#extending-further)

## Architecture

```
JMeter
  │
  ▼
HAProxy  ──────────────►  HAProxy stats (:8404/stats)  ──►  Prometheus ──► Grafana
  │                                                            ▲
  ▼                                                            │
Apache1 / Apache2 / Apache3                              cAdvisor (container CPU/mem/net)
  │
  ▼  (observed on the network interface, not "in the path")
tcpdump  ◄──────────────  started / stopped by Airflow via the Docker socket
  │
  ▼
traffic_<label>.pcap
  │
  ▼
Airflow DAG: resolve_servers → create_capture → start_capture →
             wait_for_capture → parse_and_store → archive_capture →
             cleanup_old_archives
  │
  ▼
Python parser (Scapy): packets + flows, enriched metadata
  │
  ▼
PostgreSQL: captures / servers / packets / flows
  │
  ▼
Metabase  (SQL-native analytics dashboards)
```

Two separate concerns, two separate tools, on purpose:
- **Grafana + Prometheus + cAdvisor** → infrastructure health (CPU, memory,
  network throughput, HAProxy request rate/errors/queue/backend status).
- **Metabase** → traffic analytics from the parsed packet/flow data in
  Postgres. Metabase never touches the pcap files directly — it only talks
  to Postgres, which is a cleaner separation of responsibilities.

## What you'll learn
- Docker networking & multi-container orchestration, including
  container-to-container control via the Docker socket
- Load balancing with HAProxy — round robin, least-connections,
  source-hash, and random, with a way to compare them
- Packet capture with `tcpdump`, orchestrated (not just left running)
- Parsing `.pcap` files in Python with Scapy, including flow reconstruction
  and retransmission/fragmentation detection
- Orchestrating a real multi-step ETL pipeline with Airflow (not just a
  single parsing task)
- Infrastructure monitoring with Prometheus/Grafana/cAdvisor
- SQL-native analytics with Metabase
- Resilience/performance testing: induced failures, network impairment,
  and load ramping

## Thesis / defense prep
- `docs/architecture-traffic-path.svg` and `docs/architecture-data-pipeline.svg`
  — clean architecture diagrams (matching the ASCII ones above, but
  presentation-ready) for slides or a report.
- `docs/DATA_DICTIONARY.md` — every table and column, with a one-line
  reason it exists.
- `docs/EXAMINER_QA.md` — prepared answers to the design-decision questions
  an examiner is likely to ask (why HAProxy, why Airflow, why normalize the
  schema, what happens on failure, how this would scale, etc.) — read this
  before a defense, not during one.
- `docs/AIRFLOW_STUDY_GUIDE.md` — six small standalone DAGs
  (`airflow/dags/study_*.py`), each demonstrating exactly one Airflow
  concept (dependencies, branching, parallel tasks, XCom, operator types,
  scheduling), if you need to actually learn/demonstrate how Airflow works
  rather than just run the finished pipeline.

## Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
  (includes Docker Compose)
- [Wireshark](https://www.wireshark.org/) (optional — for opening archived
  `.pcap.gz` files after decompressing them)
- [Apache JMeter](https://jmeter.apache.org/download_jmeter.cgi) or
  [`hey`](https://github.com/rakyll/hey) to generate load
- Git, and a text editor / VS Code

You do **not** need Python, PostgreSQL, or Airflow installed locally.

> **Docker socket note:** Airflow starts/stops `tcpdump` inside the
> `haproxy` container itself, via `/var/run/docker.sock` mounted into the
> Airflow containers. This is a standard (if slightly unusual-looking)
> pattern for container orchestration from within a container. On Docker
> Desktop for Windows/Mac this works out of the box. On native Linux, if
> Airflow tasks fail with a permission error on the socket, either run
> `sudo chmod 666 /var/run/docker.sock` (fine for local/demo use) or add
> your user to the `docker` group and re-login.

## Project layout
```
packetflow/
├── docker-compose.yml
├── haproxy/
│   ├── Dockerfile              # haproxy + tcpdump installed
│   ├── entrypoint.sh           # just runs haproxy; Airflow drives tcpdump
│   ├── haproxy.cfg             # active config (round robin by default)
│   └── configs/                # swappable balance-algorithm variants
│       ├── haproxy-roundrobin.cfg
│       ├── haproxy-leastconn.cfg
│       ├── haproxy-source.cfg
│       └── haproxy-random.cfg
├── apache/web{1,2,3}/index.html
├── db/
│   └── init.sql                # captures / servers / packets / flows
├── parser/
│   ├── parse_packets.py        # Scapy parsing + flow aggregation
│   └── requirements.txt
├── airflow/dags/
│   ├── packet_pipeline_dag.py  # the real pipeline
│   └── study_*.py              # 6 small DAGs for learning Airflow itself
│                                 (see docs/AIRFLOW_STUDY_GUIDE.md)
├── monitoring/
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/   # auto-provisioned datasource + dashboard
├── scripts/
│   ├── network_delay.sh        # tc netem latency/loss injection
│   ├── failure_test.sh         # kill a backend mid-traffic, observe failover
│   ├── stress_test.sh          # ramp concurrency, save results per level
│   └── benchmark.sh            # capture rate / parse+insert throughput / totals
├── docs/
│   ├── architecture-traffic-path.svg
│   ├── architecture-data-pipeline.svg
│   ├── DATA_DICTIONARY.md      # every table, every column, why it exists
│   └── EXAMINER_QA.md          # prepared answers to likely defense questions
├── dashboard/                  # OPTIONAL legacy Streamlit alternative
│                                 to Metabase (not started by default —
│                                 see "Optional: Streamlit instead of
│                                 Metabase" below)
└── README.md
```

## Phase 1 — Start everything
```bash
docker compose up --build -d
```
First run pulls several images (`httpd`, `postgres`, `apache/airflow`,
`prometheus`, `cadvisor`, `grafana`, `metabase`) — give it a few minutes.

```bash
docker compose ps
```
Expect: `web1`, `web2`, `web3`, `haproxy`, `app-db`, `airflow-db`,
`airflow-webserver`, `airflow-scheduler`, `prometheus`, `cadvisor`,
`grafana`, `metabase` all `Up` (`airflow-init` runs once and exits — that's
expected).

## Phase 2 — Verify load balancing
```bash
for i in {1..6}; do curl -s http://localhost:8080 | grep "Hello"; done
```
Responses should rotate between Server 1/2/3. You can also watch it live at
the HAProxy stats page: **http://localhost:8404/stats**

## Phase 3 — Generate traffic
**JMeter (GUI):** Thread Group → 100 users, 5s ramp-up, 100 loops → HTTP
Request → server `localhost`, port `8080`, path `/`. Click Run.

**Command line:**
```bash
hey -z 60s -c 50 http://localhost:8080/
# or
for i in {1..2000}; do curl -s -o /dev/null http://localhost:8080/; done
```

## Phase 4 — Let Airflow run the capture pipeline
Open the Airflow UI: **http://localhost:8081** (`admin` / `admin`).

The `packet_capture_pipeline` DAG runs every 5 minutes (or trigger it
manually) and executes, in order:

1. **resolve_servers** — looks up web1/web2/web3's current container IPs
   via the Docker socket and upserts them into the `servers` table.
2. **create_capture** — inserts a new row into `captures` with a versioned
   label (`capture_20260713_150000`), status `pending`.
3. **start_capture** — execs into the `haproxy` container and starts
   `tcpdump` with a built-in `timeout` (default 55s, configurable via the
   `CAPTURE_DURATION_SECONDS` Airflow Variable), so it stops itself —
   no separate "stop" step needed.
4. **wait_for_capture** — sleeps until the capture is done, plus a safety
   margin for the file to flush to disk.
5. **parse_and_store** — Scapy parses the pcap and bulk-inserts rows into
   `packets` (IP/port/protocol/length/TTL/TCP window/flags, retransmission
   and fragmentation flags, HTTP method/URL/status/host when present) and
   `flows` (5-tuple conversations: packet count, total bytes, duration).
6. **archive_capture** — gzip-compresses the raw pcap into
   `pcap/archive/<label>.pcap.gz` and deletes the uncompressed original.
7. **cleanup_old_archives** — deletes archives older than
   `ARCHIVE_RETENTION_DAYS` (default 3) so disk usage doesn't grow forever.

Check progress directly in Postgres:
```bash
docker exec -it app-db psql -U packets -d packets_db -c \
  "SELECT id, capture_label, status, packet_count, balance_algorithm FROM captures ORDER BY id DESC LIMIT 5;"
```

## Phase 5 — Infrastructure metrics: Prometheus + Grafana
- Prometheus: **http://localhost:9090** — scrapes HAProxy's `/metrics`
  endpoint (requests/sec, errors, queue depth, backend up/down) and
  cAdvisor (per-container CPU, memory, network throughput).
- Grafana: **http://localhost:3001** (`admin` / `admin`) — a starter
  dashboard **"PacketFlow - Infrastructure Overview"** is
  auto-provisioned with backend status, response rate, active sessions, CPU,
  memory, and network throughput panels. Edit or add panels freely; anything
  you build is stored in the `grafana_data` volume.

> cAdvisor's container-path mounts (`/var/lib/docker`, `/dev/disk`, etc.)
> are a standard Linux-host recipe; on Docker Desktop for Windows/Mac you
> may see partial metrics (missing disk I/O stats in particular) since
> those paths live inside Docker Desktop's own VM. CPU/memory/network
> panels still work fine.

## Phase 6 — Traffic analytics: Metabase
Open **http://localhost:3000**, run through the first-time setup wizard,
then add a database connection:
- Type: PostgreSQL
- Host: `app-db`
- Port: `5432`
- Database name: `packets_db`
- Username: `packets` / Password: `packets`

Then build questions (or paste these as native/SQL queries to get started
fast):

```sql
-- Requests handled per backend server (proves HAProxy is balancing)
SELECT server_name, COUNT(*) AS packets
FROM packets
WHERE server_name IS NOT NULL
GROUP BY server_name
ORDER BY packets DESC;
```

```sql
-- Protocol distribution
SELECT protocol, COUNT(*) FROM packets GROUP BY protocol;
```

```sql
-- Top requested URLs
SELECT http_url, COUNT(*) AS hits
FROM packets
WHERE http_url IS NOT NULL
GROUP BY http_url
ORDER BY hits DESC
LIMIT 10;
```

```sql
-- Top client IPs
SELECT src_ip, COUNT(*) AS packets
FROM packets
GROUP BY src_ip
ORDER BY packets DESC
LIMIT 10;
```

```sql
-- Retransmission rate per capture (useful with scripts/network_delay.sh)
SELECT c.capture_label,
       COUNT(*) FILTER (WHERE p.is_retransmission) * 100.0 / COUNT(*) AS retransmit_pct
FROM packets p
JOIN captures c ON c.id = p.capture_id
GROUP BY c.capture_label
ORDER BY c.capture_label DESC;
```

```sql
-- Flow summary: heaviest conversations
SELECT src_ip, dst_ip, protocol, packet_count, total_bytes, duration_seconds
FROM flows
ORDER BY total_bytes DESC
LIMIT 10;
```

Pin these as a dashboard once you've built a few — that's the whole
appeal of Metabase: no code, just SQL → chart → dashboard.

### Optional: Streamlit instead of Metabase
A hand-built Streamlit dashboard is still included under `dashboard/` if
your professor wants a custom web application rather than an off-the-shelf
BI tool. It's not started by `docker compose up` by default. To add it
back, append this service to `docker-compose.yml` and re-run
`docker compose up -d --build dashboard`:
```yaml
  dashboard:
    build: ./dashboard
    container_name: dashboard
    ports:
      - "8501:8501"
    environment:
      DB_HOST: app-db
      DB_PORT: "5432"
      DB_NAME: packets_db
      DB_USER: packets
      DB_PASSWORD: packets
    depends_on:
      app-db:
        condition: service_healthy
    networks: [appnet]
```
Note it queries the old flat `packets` schema shape — it'll run, but won't
show `server_name`/flows/capture-level fields until you extend its SQL to
match the new schema.

## Comparing load balancing algorithms
```bash
# swap the active config, e.g. to least-connections
cp haproxy/configs/haproxy-leastconn.cfg haproxy/haproxy.cfg
docker compose restart haproxy

# tell Airflow so new captures are labeled correctly
docker exec airflow-webserver airflow variables set BALANCE_ALGORITHM leastconn
```
Generate the same load again (Phase 3), let a couple of DAG runs complete,
then compare `captures.balance_algorithm` against `flows`/`packets` in
Metabase — e.g. group the "requests per server" query above by joining in
`captures.balance_algorithm` to see how distribution shape changes between
`roundrobin`, `leastconn`, `source`, and `random`.

## Resilience & performance experiments (Tier 3)
These are one-off experiments you run and observe, not always-on services.

```bash
# Kill web2 mid-traffic, confirm HAProxy fails over with no downtime
./scripts/failure_test.sh web2

# Add 200ms latency + 5% packet loss to web3, then generate traffic and
# watch retransmission rates climb in the query above
./scripts/network_delay.sh web3 200ms 5%
# undo it:
docker exec web3 tc qdisc del dev eth0 root netem

# Ramp concurrency (100/500/1000/5000 users) and save results for comparison
./scripts/stress_test.sh

# Report real capture/parse/insert throughput numbers instead of "fast"
./scripts/benchmark.sh
```

## Inspecting the database directly
```bash
docker exec -it app-db psql -U packets -d packets_db -c \
  "SELECT protocol, COUNT(*) FROM packets GROUP BY protocol;"
```

## Stopping / resetting
```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop and wipe all volumes (pcaps, postgres, grafana, metabase, airflow)
```

## Troubleshooting
| Symptom | Fix |
|---|---|
| `airflow-webserver`/`scheduler` restart in a loop | Wait for `airflow-init` (`db migrate` + admin user + default Variables) to finish first. `docker compose logs airflow-init`. |
| `start_capture` task fails with a Docker permission error | See the Docker socket note above — `sudo chmod 666 /var/run/docker.sock` on native Linux, or confirm Docker Desktop is running on Windows/Mac. |
| No rows in `packets` after a DAG run | Check `docker compose logs airflow-scheduler` for the `parse_and_store` task; also confirm traffic was actually generated *during* the ~55s capture window for that run. |
| `captures.status = 'error'` | Check `captures.error_message` — most commonly a missing/corrupted pcap (capture window too short, or the DAG was triggered before HAProxy was healthy). |
| Metabase says "No tables" | Confirm the `app-db` connection details match exactly (`app-db`, not `localhost`, since Metabase runs inside the same Docker network). |
| Grafana panels show "No data" | Prometheus needs a few scrape intervals (5s) after startup, and HAProxy needs traffic flowing to produce non-zero request-rate panels. |
| cAdvisor fails to start / restarts | Common on some Docker Desktop configurations due to host path mounts; safe to `docker compose stop cadvisor` — Grafana's HAProxy panels still work without it. |
| Port already in use (8080/8081/8404/9090/3000/3001/5433) | Change the left side of the relevant `ports:` mapping in `docker-compose.yml`. |

## Extending further
- Add a `pgAdmin` or `Adminer` service for a lightweight Postgres GUI.
- Feed HAProxy's own structured logs (not just packets) into the pipeline
  for an application-log + network-log correlation view.
- Swap the retransmission heuristic (duplicate seq numbers) for full RTT
  estimation using SYN/ACK timing per flow.
- Tier 4 (only after everything above is solid): anomaly detection on the
  `flows` table, or a RAG layer over packet/flow metadata for natural
  -language queries like "which client generated the most retransmissions
  yesterday?"

## CI

Two GitHub Actions workflows run on every push/PR to `main`:
- **CI** (`.github/workflows/ci.yml`) — validates `docker-compose.yml`,
  lints the Python (`parser/`, `dashboard/`, `airflow/dags/`) with flake8,
  lints shell scripts with ShellCheck, and lints Dockerfiles with Hadolint.
- **Docker Build** (`.github/workflows/docker-build.yml`) — builds the
  `haproxy`, `airflow`, and `dashboard` images to catch Dockerfile
  regressions early.

## Author
Adithya Rao K — M.E. Computer Science, Manipal School of Information
Sciences (MSIS).
