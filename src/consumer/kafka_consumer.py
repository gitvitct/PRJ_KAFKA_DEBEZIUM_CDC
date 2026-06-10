import json

from confluent_kafka import Consumer

from src.consumer.config import KAFKA_CONFIG
from src.consumer.analytics_writer import AnalyticsWriter


TOPIC = "cdc.public.users"


class CDCConsumer:

    def __init__(self):

        self.consumer = Consumer(KAFKA_CONFIG)

        self.consumer.subscribe([TOPIC])

        self.writer = AnalyticsWriter()

        print(f"Subscribed to topic: {TOPIC}")

    def run(self):

        while True:

            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:

                data = json.loads(
                    msg.value().decode("utf-8")
                )

                payload = data["payload"]

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
                    f"Error processing message: {e}"
                )