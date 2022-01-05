from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import Deserializer

class AvroService:

    def __init__(self) -> None:
        pass

    def get_avro_schema(self):
        sm_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
        subjects = sm_client.get_subjects()
        return sm_client.get_latest_version('pagamentos-pix-recebido-value').schema.schema_str
