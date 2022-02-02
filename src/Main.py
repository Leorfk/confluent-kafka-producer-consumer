from util import logging_config
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import Schema
import logging
logging_config.init_logs(logging.INFO)
from confluent_kafka.schema_registry import RegisteredSchema
import json

def serialize_event(event: dict, memis):
    return event


def lo_xap(toma, deita):
    return 'pagamentos-pix-devolucao-emitida-value'


is_kafka_in_use = True
if is_kafka_in_use:
    sm_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
    avro: Schema  = sm_client.get_latest_version('pagamentos-pix-devolucao-emitida-value').schema
    print(json.loads(avro.schema_str)['namespace'])
    from service.teste_serializer_deserializer import o_abafa
    o_abafa()
