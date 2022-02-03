from toma_kafka.producer.producer_config import ProducerConfig
from toma_kafka.producer.producer_avro import ProducerAvro
from confluent_kafka.schema_registry import SchemaRegistryClient
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
conf = ProducerConfig(sm_client).get_serializing_producer_configs(
    kafka_broker='127.0.0.1:9092', subject_name='pagamentos-pix-emitido-value')
producer = ProducerAvro(conf)

producer.send_message(topico='pagamentos-pix-emitido',
                      evento=evento, header=header)
