# Real-Time Analytics Pipeline

This repository contains a local real-time analytics system built around Kafka, Spark Structured Streaming, PostgreSQL, and Streamlit. It simulates user activity, processes events continuously, materializes minute-level metrics, and exposes them through a dashboard that updates automatically.

## Overview

- A Kafka producer emits high-volume JSON events with retries, controlled duplicate generation, and a small malformed-event rate for data-quality validation.
- Kafka buffers the event stream on a dedicated topic with three partitions.
- Spark consumes the topic, filters invalid payloads, removes duplicates by `event_id`, applies watermarking, and computes minute-level aggregates.
- PostgreSQL stores windowed analytics with an upsert strategy so updated windows do not create duplicate rows.
- Invalid or malformed events are persisted into a dead-letter table for inspection instead of being silently ignored.
- Streamlit reads the metrics table and refreshes the dashboard automatically.

## Architecture

```text
                         +-----------------------------+
                         |      Python Producer        |
                         |  user activity simulator    |
                         |  retries + duplicate rate   |
                         +-------------+---------------+
                                       |
                                       v
                         +-----------------------------+
                         |       Kafka: events         |
                         |      3 partitions topic     |
                         | durable event buffer        |
                         +-------------+---------------+
                                       |
                                       v
                    +-----------------------------------------+
                    |     Spark Structured Streaming          |
                    | JSON parsing + validation               |
                    | dedupe on event_id + watermark          |
                    | minute windows + anomaly detection      |
                    +-------------+---------------+-----------+
                                  |               |
                         valid aggregates   invalid payloads
                                  |               |
                                  v               v
                 +---------------------------+   +---------------------------+
                 | PostgreSQL: analytics     |   | PostgreSQL: dead_letter   |
                 | revenue / active users    |   | invalid payload + reason  |
                 | event_count / anomaly     |   | data quality inspection   |
                 +-------------+-------------+   +---------------------------+
                               |
                               v
                    +-----------------------------+
                    |    Streamlit Dashboard      |
                    | KPI cards + trends + DQ     |
                    | auto-refresh every 5 sec    |
                    +-----------------------------+
```

The pipeline has two outputs from the Spark layer:

- validated events are aggregated into minute-level serving metrics
- malformed events are written to a dead-letter table for traceability

That split is intentional. It keeps the analytics path clean without hiding bad source data.

## Tech Stack

- Python 3.11
- Apache Kafka
- Apache Zookeeper
- PySpark Structured Streaming
- PostgreSQL 16
- Streamlit
- Docker Compose

## Project Layout

```text
real-time-analytics-pipeline/
|-- producer/
|   `-- producer.py
|-- processing/
|   `-- spark_streaming.py
|-- dashboard/
|   `-- app.py
|-- db/
|   `-- schema.sql
|-- config/
|   `-- config.yaml
|-- utils/
|   `-- logger.py
|-- docker/
|   |-- Dockerfile.dashboard
|   `-- Dockerfile.spark
|-- tests/
|   |-- test_dashboard.py
|   |-- test_processing.py
|   `-- test_producer.py
|-- .github/workflows/
|   `-- ci.yml
|-- docker-compose.yml
|-- requirements.txt
|-- README.md
`-- architecture.txt
```

## Setup

### Prerequisites

- Docker Desktop with Compose support

### Run the stack

From the repository root:

```bash
docker compose up --build
```

Services:

- Kafka broker: `localhost:9092`
- PostgreSQL: `localhost:5432`
- Streamlit dashboard: `http://127.0.0.1:8501`

## What the services do

- `producer` starts sending events to Kafka immediately after the topic is created.
- `spark` starts structured streaming queries and writes minute-level aggregates plus dead-letter rows to PostgreSQL every 10 seconds.
- `dashboard` refreshes every 5 seconds and reads the latest metric windows from the database.

## How to Verify It

1. Start the stack with `docker compose up --build`.
2. Open `http://127.0.0.1:8501`.
3. Confirm the metric cards and charts begin filling in after a short delay.
4. Query PostgreSQL:

```bash
docker exec -it rta-postgres psql -U analytics_user -d analytics
```

```sql
SELECT timestamp, total_revenue, active_users, event_count, anomaly_detected
FROM analytics_metrics
ORDER BY timestamp DESC
LIMIT 10;
```

```sql
SELECT kafka_timestamp, invalid_reason, raw_payload
FROM dead_letter_events
ORDER BY created_at DESC
LIMIT 10;
```

## Common Recovery Step

If Kafka fails at startup with a `NodeExistsException`, clean up the old broker session and start from a fresh local state:

```bash
docker compose down -v
docker compose up --build
```

That clears the stale broker registration and recreates the Kafka and PostgreSQL state for a clean run.

## Example Event

```json
{
  "event_id": "c1d1b435-837e-4f7d-b6fc-8a3f46fe2626",
  "user_id": 184,
  "event_type": "payment",
  "timestamp": "2026-04-06T09:15:10",
  "amount": 412.75
}
```

## Example Aggregated Output

```text
timestamp           total_revenue  active_users  event_count  anomaly_detected
2026-04-06 09:15    12874.50       822           1410         true
2026-04-06 09:16    11721.10       801           1388         true
2026-04-06 09:17     4560.20       779           1361         false
```

## Scaling Notes

- Increase Kafka partitions to improve producer and consumer parallelism.
- Move Spark from `local[*]` to a real cluster when throughput grows beyond a single container.
- Replace the PostgreSQL sink with a warehouse or OLAP store if retention and query volume increase.
- Tune `maxOffsetsPerTrigger`, checkpoint storage, and watermark duration based on event lateness and target latency.

## Future Improvements

- Add Prometheus metrics for service health and lag tracking.
- Split aggregates into separate fact tables for richer dashboard queries.
- Add schema validation with a registry-backed serializer.
- Move secrets out of `config.yaml` into environment-specific secret management.

## Environment Overrides

For local demos the YAML file is enough, but the services also support environment variable overrides so the same code can be pointed at different brokers or databases without editing source-controlled config.

Supported overrides include:

- `RTA_CONFIG_PATH`
- `RTA_KAFKA_BROKERS`
- `RTA_KAFKA_TOPIC`
- `RTA_POSTGRES_HOST`
- `RTA_POSTGRES_PORT`
- `RTA_POSTGRES_DB`
- `RTA_POSTGRES_USER`
- `RTA_POSTGRES_PASSWORD`
- `RTA_PRODUCER_CONTINUOUS`
- `RTA_PRODUCER_TARGET_EVENTS`
