##################################################################################################################

kafka-cdc-avro-platform/
│
├── docker/
│   ├── docker-compose.yml
│   ├── postgres/
│   │   └── init.sql
│   ├── connect/
│   │   └── debezium-connector.json
│
├── schemas/
│   └── users.avsc
│
├── src/
│   ├── consumer/
│   │   ├── cli.py
│   │   ├── kafka_consumer.py
│   │   ├── avro_deserializer.py
│   │   └── config.py
│   │
│   ├── producer/ (opcional demo)
│   │   └── mock_writer.py
│   │
│   ├── utils/
│   │   ├── logger.py
│   │   └── json_formatter.py
│
├── tests/
│   ├── test_deserializer.py
│   ├── test_consumer.py
│
├── requirements.txt
├── README.md
└── Makefile



##################################################################################################################
# Kafka CDC Avro Platform

End-to-end data platform using:

- PostgreSQL (CDC source)
- Debezium (Change Data Capture)
- Kafka (event streaming)
- Schema Registry (Avro governance)
- Python CLI consumer

## Features
- Real-time CDC ingestion
- Avro serialization
- Event-driven architecture
- CLI consumer for Kafka topics

## Run

docker compose up -d

python src/consumer/cli.py --topic cdc.public.users --group consumer-1