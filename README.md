# 🚀 Kafka CDC Analytics Platform

Plataforma de captura de mudanças em banco de dados (CDC - Change Data Capture) utilizando PostgreSQL, Debezium, Apache Kafka e PostgreSQL Analytics.

O projeto demonstra uma arquitetura orientada a eventos capaz de capturar alterações em tempo real de uma tabela PostgreSQL, publicar eventos no Kafka e persisti-los em uma base analítica.

---

# 📐 Arquitetura

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
```

---

# 🛠 Tecnologias Utilizadas

- PostgreSQL 15
- Apache Kafka
- Zookeeper
- Debezium
- Kafka Connect
- Schema Registry
- Python 3
- Confluent Kafka
- Psycopg2
- Docker Compose

---

# 📂 Estrutura do Projeto

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

# 🎯 Objetivo

Capturar alterações realizadas na tabela:

```sql
public.users
```

e transformá-las em eventos Kafka através do Debezium.

Os eventos são consumidos por uma aplicação Python que grava todas as operações em uma base analítica.

---

# 🔄 Fluxo dos Dados

### 1. Inserção/Atualização no PostgreSQL

```sql
INSERT INTO users(name,email)
VALUES ('João','joao@email.com');
```

### 2. Debezium captura a alteração

O Debezium monitora o WAL (Write Ahead Log) do PostgreSQL.

### 3. Evento publicado no Kafka

Tópico:

```text
cdc.public.users
```

### 4. Consumer processa o evento

A aplicação Python:

- Consome mensagens Kafka
- Identifica a operação (Insert, Update ou Delete)
- Extrai os dados relevantes
- Persiste em PostgreSQL Analytics

### 5. Evento armazenado

Tabela:

```sql
cdc_events
```

---

# 📦 Serviços Docker

O ambiente sobe automaticamente:

| Serviço | Porta |
|----------|---------|
| PostgreSQL CDC | 5432 |
| PostgreSQL Analytics | 5433 |
| Kafka | 9092 |
| Zookeeper | 2181 |
| Schema Registry | 8081 |
| Kafka Connect | 8083 |

---

# 🚀 Como Executar

## 1. Clonar o projeto

```bash
git clone <repo-url>
cd kafka-cdc-platform
```

---

## 2. Subir infraestrutura

```bash
cd docker

docker compose up -d
```

Verificar containers:

```bash
docker ps
```

---

## 3. Registrar Connector Debezium

Caso não seja realizado automaticamente:

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connect/debezium-connector.json
```

---

## 4. Executar Consumer

Dentro do container Python:

```bash
python src/consumer/cli.py
```

---

# 🧪 Testando CDC

Conecte no PostgreSQL principal:

```bash
docker exec -it postgres_cdc psql -U postgres -d app_db
```

Inserir registro:

```sql
INSERT INTO users(name,email)
VALUES ('Maria','maria@email.com');
```

Atualizar:

```sql
UPDATE users
SET email='novo@email.com'
WHERE id=1;
```

Excluir:

```sql
DELETE FROM users
WHERE id=1;
```

---

# 📥 Exemplo de Evento Debezium

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "name": "Maria",
      "email": "maria@email.com"
    },
    "op": "c",
    "source": {
      "table": "users"
    }
  }
}
```

---

# 📊 Base Analítica

Todos os eventos são persistidos na tabela:

```sql
cdc_events
```

Estrutura:

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

# 🔍 Operações Capturadas

| Código | Operação |
|----------|-----------|
| c | Create |
| u | Update |
| d | Delete |
| r | Snapshot Read |

---

# 📈 Possíveis Evoluções

- Integração com Grafana
- Dashboards em tempo real
- Apache Avro completo com Schema Registry
- Apache Spark Streaming
- Apache Flink
- Data Lake (S3/MinIO)
- Kubernetes Deployment
- Observabilidade com Prometheus

---

# 👨‍💻 Autor

Desenvolvido para demonstrar uma arquitetura moderna de CDC baseada em eventos utilizando:

- PostgreSQL
- Debezium
- Kafka
- Python

---