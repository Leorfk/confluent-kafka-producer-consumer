from uuid import uuid4
from confluent_kafka import SerializingProducer
from service.avro_service import AvroService
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

def serialize_event(event: dict, memis):
    return event

evento = {
    "numero_agencia": "toma",
    "numero_conta": "toma",
    "digito_verificador": "toma",
    "valor_transacao_financeira": "toma",
    "data_contabilizacao": "toma",
    "codigo_identificador_cliente": "toma"
}

sm_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_str = AvroService().get_avro_schema(
    sm_client, 'pagamentos-pix-emitido-value')
serializer = AvroSerializer(
    schema_registry_client=sm_client,
    schema_str=avro_str,
    to_dict=serialize_event)

serializer(evento, None)
producer_configs = {'bootstrap.servers': '127.0.0.1:9092',
                'key.serializer': StringSerializer('utf_8'),
                'value.serializer': serializer}
producer = SerializingProducer(producer_configs)

producer.produce('pagamentos-pix-emitido', value=evento, key=str(uuid4()))
producer.flush()