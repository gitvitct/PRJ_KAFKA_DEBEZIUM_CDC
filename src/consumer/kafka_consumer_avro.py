from confluent_kafka import DeserializingConsumer

from confluent_kafka.schema_registry import (
    SchemaRegistryClient
)

from confluent_kafka.schema_registry.avro import (
    AvroDeserializer
)

from src.consumer.analytics_writer import AnalyticsWriter


TOPIC = "cdc.public.users"


class CDCConsumerAvro:

    def __init__(self):

        schema_registry_conf = {
            "url": "http://schema-registry:8081"
        }

        schema_registry_client = SchemaRegistryClient(
            schema_registry_conf
        )

        avro_deserializer = AvroDeserializer(
            schema_registry_client
        )

        consumer_conf = {
            "bootstrap.servers": "kafka:29092",
            "group.id": "cdc-consumer-avro",
            "auto.offset.reset": "earliest"
        }

        self.consumer = DeserializingConsumer(
            {
                **consumer_conf,
                "value.deserializer": avro_deserializer
            }
        )

        self.consumer.subscribe([TOPIC])

        self.writer = AnalyticsWriter()

    def run(self):

        while True:

            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(msg.error())
                continue

            try:

                data = msg.value()

                payload = data

                operation = payload["op"]

                source_table = payload["source"]["table"]

                after = payload.get("after")
                before = payload.get("before")

                record_id = None

                if after:
                    record_id = after.get("id")

                elif before:
                    record_id = before.get("id")

                self.writer.save_event(
                    operation,
                    source_table,
                    record_id,
                    payload
                )

                print(
                    f"CDC Event Saved | "
                    f"op={operation} | "
                    f"table={source_table} | "
                    f"id={record_id}"
                )

            except Exception as e:

                print(
                    f"Error processing AVRO message: {e}"
                )