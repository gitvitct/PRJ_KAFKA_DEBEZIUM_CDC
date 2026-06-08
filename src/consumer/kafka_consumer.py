from confluent_kafka import Consumer

def create_consumer(group_id: str):
    return Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    })