from toma_kafka.consumer.consumer_generic import ConsumerGeneric
from toma_kafka.consumer.consumer_config import ConsumerConfig
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

sm_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
deserializer = AvroDeserializer(sm_client)
config = ConsumerConfig()
conf = config.get_consumer_settings()
consumer = ConsumerGeneric(conf).create_consumer()
consumer.subscribe(['pagamentos-pix-emitido'])

def simple_consumer():
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        message = deserializer(msg.value(), None)
        print(msg.value())
        print(message)

def batch_consumer():
    while True:
        msgs = consumer.consume(100, 1)
        for msg in msgs:
            if msg is None:
                continue
            message = deserializer(msg.value(), None)
            print(msg.value())
            print(message)
        consumer.commit()

batch_consumer()