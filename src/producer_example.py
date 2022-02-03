from toma_kafka.producer.producer_config import ProducerConfig
from toma_kafka.producer.producer_avro import ProducerAvro
from toma_kafka.producer.producer_generic import ProducerGeneric
from confluent_kafka.schema_registry import (SchemaRegistryClient,
                                             topic_subject_name_strategy,
                                             topic_record_subject_name_strategy,
                                             record_subject_name_strategy,
                                             reference_subject_name_strategy)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext
from uuid import uuid4

evento = {
    "numero_agencia": "toma",
    "numero_conta": "toma",
    "digito_verificador": "toma",
    "valor_transacao_financeira": "toma",
    "data_contabilizacao": "toma",
    "codigo_identificador_cliente": "toma"
}
header = {
    'id': str(uuid4()),
    'transactionid': str(uuid4()),
    'correlationid': str(uuid4()),
    'type': 'pagamentos-pix-emitido-value'
}
sm_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
config = ProducerConfig(sm_client)
conf_avro = config.get_serializing_producer_configs(
    kafka_broker='127.0.0.1:9092', subject_name='pagamentos-pix-emitido-value')

producer = ProducerAvro(conf_avro)
producer.send_message(topico='pagamentos-pix-emitido',
                      evento=evento, header=header)

conf_generic = config.get_generic_producer_configs('127.0.0.1:9092')

def custom_go_horse_strategy(ctx, toma):
    topic = toma.split('.')[0].replace("_", "-")
    ctx = SerializationContext(topic, 'value')
    return ctx.topic + "-" + ctx.field

schema_str = sm_client.get_latest_version(
    'pagamentos-pix-emitido-value').schema.schema_str
serializer_conf = {'auto.register.schemas': False,
                   'subject.name.strategy': custom_go_horse_strategy}
serializer = AvroSerializer(schema_registry_client=sm_client,
                            schema_str=schema_str, conf=serializer_conf)

evento_bytes = serializer(evento, None)
print(evento_bytes)
producer = ProducerGeneric(conf_generic)
producer.send_message(topico='pagamentos-pix-emitido',
                      evento=evento_bytes, header=header)
