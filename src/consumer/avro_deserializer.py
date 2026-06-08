from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

def get_deserializer(schema_registry_url):
    client = SchemaRegistryClient({"url": schema_registry_url})
    return AvroDeserializer(client)