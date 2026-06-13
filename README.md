# 🚀 Kafka CDC Analytics Platform

A Change Data Capture (CDC) platform built with PostgreSQL, Debezium, Apache Kafka, and PostgreSQL Analytics.

This project demonstrates an event-driven architecture capable of capturing database changes in real time, streaming them through Kafka, and storing them in an analytical database for further processing and reporting.

---

# 📐 Architecture

```text
PostgreSQL (OLTP)
        │
        │ CDC (WAL)
        ▼
    Debezium
        │
        ▼
      Kafka
        │
        ▼
 Python Consumer
        │
        ▼
PostgreSQL Analytics
        │
        ▼
Grafana CDC Dashboard
```

---

# 🛠 Technologies

- PostgreSQL 15
- Apache Kafka
- Apache Zookeeper
- Debezium
- Kafka Connect
- Schema Registry
- Python 3
- Confluent Kafka
- Psycopg2
- Docker Compose
- Grafana

---

# 📂 Project Structure

```text
kafka-cdc-platform/
│
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
│       └── debezium-connector.json
│
├── schemas/
│   └── users.avsc
│ 
├── grafana/
│   ├── dashboards/
│   │   └── cdc_dashboard.json
│   │
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboard.yml
│       │
│       └── datasources/
│           └── datasource.yml
│
├── src/
│   ├── consumer/
│   │   ├── cli.py
│   │   ├── kafka_consumer.py
│   │   ├── analytics_writer.py
│   │   ├── avro_deserializer.py
│   │   └── config.py
│   │
│   ├── producer/
│   │   └── mock_writer.py
│   │
│   └── utils/
│       ├── logger.py
│       └── json_formatter.py
│
└── README.md
```

---

# 🎯 Project Goal

Capture changes from the following PostgreSQL table:

```sql
public.users
```

and transform them into Kafka events using Debezium.

These events are consumed by a Python application and stored in an analytics database for auditing, reporting, and real-time data processing.

---

# 🔄 Data Flow

### 1. Insert or Update Data

```sql
INSERT INTO users(name, email)
VALUES ('John Doe', 'john@example.com');
```

### 2. Debezium Captures the Change

Debezium monitors PostgreSQL's Write-Ahead Log (WAL) and detects database changes.

### 3. Event Published to Kafka

Topic:

```text
cdc.public.users
```

### 4. Python Consumer Processes the Event

The consumer application:

- Reads messages from Kafka
- Identifies the operation type (Create, Update, Delete)
- Extracts relevant payload data
- Persists the event into the analytics database

### 5. Event Stored for Analytics

Target table:

```sql
cdc_events
```

---

# 📦 Docker Services

The Docker environment automatically starts:

| Service | Port |
|----------|---------|
| PostgreSQL CDC | 5432 |
| PostgreSQL Analytics | 5433 |
| Kafka Broker | 9092 |
| Zookeeper | 2181 |
| Schema Registry | 8081 |
| Kafka Connect | 8083 |

---

# 🚀 Getting Started

## 1. Clone the Repository

```bash
git clone <repository-url>
cd kafka-cdc-platform
```

---

## 2. Start the Infrastructure

```bash
cd docker

docker compose up -d
```

Verify all containers are running:

```bash
docker ps
```

---

## 3. Register the Debezium Connector

If not automatically registered:

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connect/debezium-connector.json
```

---

## 4. Start the Consumer

```bash
python src/consumer/cli.py
```

---

## 📊 Grafana Dashboard

The project includes a pre-configured Grafana dashboard for CDC monitoring.

### Dashboard Features

- Total CDC Events
- Total Inserts
- Total Updates
- Total Deletes
- Last Event Received
- Events by Operation
- Events by Source Table
- Events Per Minute
- CDC Timeline
- Latest CDC Events

---


# 🧪 Testing CDC

Connect to PostgreSQL:

```bash
docker exec -it postgres_cdc psql -U postgres -d app_db
```

Create a record:

```sql
INSERT INTO users(name,email)
VALUES ('Maria','maria@example.com');
```

Update a record:

```sql
UPDATE users
SET email='updated@example.com'
WHERE id=1;
```

Delete a record:

```sql
DELETE FROM users
WHERE id=1;
```

---

# 📥 Sample Debezium Event

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "name": "Maria",
      "email": "maria@example.com"
    },
    "op": "c",
    "source": {
      "table": "users"
    }
  }
}
```

---

# 📊 Analytics Database

All captured events are stored in:

```sql
cdc_events
```

Schema example:

```sql
CREATE TABLE cdc_events (
    id SERIAL PRIMARY KEY,
    operation VARCHAR(10),
    source_table VARCHAR(100),
    record_id INTEGER,
    payload JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

---

# 🔍 Captured Operations

| Code | Operation |
|--------|-----------|
| c | Create |
| u | Update |
| d | Delete |
| r | Snapshot Read |

---

# ✨ Future Improvements

- Grafana dashboards
- Real-time monitoring
- Full Avro serialization support
- Advanced Schema Registry integration
- Apache Spark Streaming
- Apache Flink processing
- Data Lake integration (S3 / MinIO)
- Kubernetes deployment
- Prometheus observability
- Dead Letter Queue (DLQ) implementation

---

# 🏗 Key Concepts Demonstrated

- Change Data Capture (CDC)
- Event-Driven Architecture
- Data Streaming
- Kafka Messaging
- Database Replication
- Analytics Data Pipeline
- Real-Time Processing
- Microservices Integration

---

# 👨‍💻 Author

Built as a practical demonstration of a modern CDC architecture using:

- PostgreSQL
- Debezium
- Apache Kafka
- Python
- Docker

---

## ⭐ If you found this project useful, consider giving it a star.