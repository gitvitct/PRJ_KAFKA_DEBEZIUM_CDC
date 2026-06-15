# 🚀 Real-Time CDC Platform with PostgreSQL, Debezium, Kafka, Python, and Grafana

A Data Engineering project demonstrating a complete Change Data Capture (CDC) pipeline using PostgreSQL, Debezium, Apache Kafka, Python, and Grafana.

This solution captures database changes in real time from PostgreSQL transaction logs (WAL), streams them through Kafka, processes them with Python consumers, and stores operational metrics for monitoring and analytics.

---

# 📖 Overview

Modern data platforms increasingly rely on event-driven architectures to move data in real time.

This project demonstrates how to:

- Capture database changes using CDC
- Stream events through Apache Kafka
- Process events with Python
- Store analytics data
- Monitor the entire pipeline with Grafana
- Deploy everything using Docker Compose

---

# 🏗 Architecture

```text
┌─────────────────────┐
│ PostgreSQL (OLTP)   │
└──────────┬──────────┘
           │
           │ WAL (Logical Replication)
           ▼
┌─────────────────────┐
│      Debezium       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│       Kafka         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Python Consumer   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Analytics Database  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      Grafana        │
└─────────────────────┘
```

---

# 🎯 Project Goals

This project demonstrates practical experience with:

- Change Data Capture (CDC)
- Event-Driven Architecture
- Apache Kafka
- Debezium
- Kafka Connect
- Schema Registry
- PostgreSQL Logical Replication
- Docker Compose
- Python Event Processing
- Data Observability
- Real-Time Analytics

---

# 🛠 Technology Stack

| Component | Technology |
|------------|------------|
| Source Database | PostgreSQL 15 |
| CDC Engine | Debezium |
| Streaming Platform | Apache Kafka |
| Coordination | ZooKeeper |
| Connector Framework | Kafka Connect |
| Schema Management | Confluent Schema Registry |
| Event Consumer | Python |
| Monitoring | Grafana |
| Containerization | Docker Compose |

---

# 📂 Project Structure

```text
project/

├── docker/
│   ├── docker-compose.yml
│   ├── bootstrap.sh
│   │
│   ├── postgres/
│   │   └── init.sql
│   │
│   ├── analytics/
│   │   └── init.sql
│   │
│   └── connect/
│       ├── debezium-connector.json
│       └── debezium-connector-avro.json
│
├── schemas/
│   └── users.avsc
│
├── grafana/
│   ├── dashboards/
│   └── provisioning/
│
├── src/
│   ├── consumer/
│   ├── generator/
│   ├── producer/
│   └── utils/
│
└── README.md
```

---

# 🚀 Getting Started

## Prerequisites

- Docker
- Docker Compose
- Python 3.11+
- Git

---

# 📦 Start the Environment

Navigate to the Docker directory:

```bash
cd docker
```

Start all services:

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

---

# ⚡ Bootstrap

The project includes a bootstrap script responsible for:

- Waiting for all services to become healthy
- Registering Debezium connectors
- Validating Kafka Connect availability
- Initializing CDC configuration

Run:

```bash
chmod +x bootstrap.sh

./bootstrap.sh
```

---

# 🌐 Available Services

| Service | URL / Port |
|----------|------------|
| PostgreSQL CDC | localhost:5432 |
| PostgreSQL Analytics | localhost:5433 |
| Kafka Broker | localhost:9092 |
| Kafka Connect | localhost:8083 |
| Schema Registry | localhost:8081 |
| Grafana | localhost:3000 |

---

# 🔧 Register Debezium Connector

If manual registration is required:

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connect/debezium-connector.json
```

Verify:

```bash
curl http://localhost:8083/connectors
```

Expected:

```json
[
  "postgres-users-connector"
]
```

---

# 🗄 Source Table

The CDC process monitors the following table:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

# 🔄 CDC Event Flow

## INSERT

```sql
INSERT INTO users(name,email)
VALUES ('John Doe','john@example.com');
```

Expected Debezium Operation:

```json
{
  "op":"c"
}
```

---

## UPDATE

```sql
UPDATE users
SET email='new_email@example.com'
WHERE id=1;
```

Expected Operation:

```json
{
  "op":"u"
}
```

---

## DELETE

```sql
DELETE FROM users
WHERE id=1;
```

Expected Operation:

```json
{
  "op":"d"
}
```

---

# 📡 Kafka Topics

List all topics:

```bash
docker exec kafka kafka-topics \
--bootstrap-server localhost:9092 \
--list
```

Expected topic:

```text
cdc.public.users
```

---

# 📥 Consume Events

Run the consumer:

```bash
python src/consumer/main.py
```

Or inspect logs:

```bash
docker logs -f consumer
```

---

# 🧪 CDC Validation Tests

## Test 1 – Connector Status

```bash
curl http://localhost:8083/connectors/postgres-users-connector/status
```

Expected:

```json
"RUNNING"
```

---

## Test 2 – Verify Topic Creation

```bash
docker exec kafka kafka-topics \
--bootstrap-server localhost:9092 \
--list
```

Expected:

```text
cdc.public.users
```

---

## Test 3 – INSERT Event

Execute:

```sql
INSERT INTO users(name,email)
VALUES ('CDC TEST','cdc@test.com');
```

Consume messages:

```bash
docker exec kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic cdc.public.users \
--from-beginning
```

Expected:

```json
"op":"c"
```

---

## Test 4 – UPDATE Event

Execute:

```sql
UPDATE users
SET email='updated@test.com'
WHERE id=1;
```

Expected:

```json
"op":"u"
```

---

## Test 5 – DELETE Event

Execute:

```sql
DELETE FROM users
WHERE id=1;
```

Expected:

```json
"op":"d"
```

---

## Test 6 – Verify Analytics Database

Connect:

```bash
docker exec -it postgres_analytics psql \
-U analytics \
-d analytics_db
```

Query:

```sql
SELECT *
FROM cdc_events
ORDER BY created_at DESC;
```

Expected:

Records generated from CDC events.

---

## Test 7 – Verify Grafana Dashboard

Open:

```text
http://localhost:3000
```

Validate:

- Total CDC Events
- Insert Events
- Update Events
- Delete Events
- Snapshot Events
- Event Timeline
- Latest CDC Activity

---

# 📊 Debezium Operation Codes

| Code | Description |
|--------|------------|
| c | Create (INSERT) |
| u | Update |
| d | Delete |
| r | Snapshot |

---

# 🔍 Troubleshooting

## Check PostgreSQL Logical Replication

```sql
SHOW wal_level;
```

Expected:

```text
logical
```

---

## Check Replication Slots

```sql
SELECT * FROM pg_replication_slots;
```

Expected:

```text
debezium_slot
```

---

## Check Kafka Connect Logs

```bash
docker logs kafka_connect
```

---

## Check Kafka Topics

```bash
docker exec kafka kafka-topics \
--bootstrap-server localhost:9092 \
--list
```

---

# 📈 Future Enhancements

Potential extensions for a production-grade data platform:

- Apache Spark Structured Streaming
- PySpark Transformations
- Data Lake (MinIO)
- Delta Lake
- Apache Airflow
- dbt
- Snowflake
- Prometheus
- Kubernetes
- CI/CD with GitHub Actions
- Dead Letter Queue (DLQ)
- Data Quality Validation Framework

---

# 💼 Skills Demonstrated

This project demonstrates hands-on experience with:

- Apache Kafka
- Debezium CDC
- Kafka Connect
- PostgreSQL
- Event Streaming
- Real-Time Data Pipelines
- Docker
- Python
- Schema Registry
- Avro
- Data Engineering
- Streaming Architectures
- CDC Architectures
- Data Observability
- Grafana

---

# 👨‍💻 Author

Developed as a Data Engineering portfolio project focused on real-time streaming architectures and Change Data Capture (CDC).

Core Stack:

- PostgreSQL
- Debezium
- Apache Kafka
- Python
- Docker
- Grafana