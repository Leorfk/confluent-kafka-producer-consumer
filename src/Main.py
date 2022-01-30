from service.poc_kafka_xap import consumir
from util import logging_config
from uuid import uuid4
from confluent_kafka import SerializingProducer
from service.avro_service import AvroService
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from service.Gerador import generateData
import logging
import json
logging_config.init_logs(logging.INFO)


def serialize_event(event: dict, memis):
    return event


def lo_xap(toma, deita):
    return 'pagamentos-pix-recebido-value'


is_kafka_in_use = True
if is_kafka_in_use:
    # logging.info(libversion())
    # logging.info(version())
    # kafka_confluent_producer.gerar_massa('pagamentos-pix-emitido')
    # consumir('pagamentos-pix-emitido')
    # kafka_confluent_consumer.consume(['pagamentos-pix-emitido'])

    # evento = {
    #     "numero_agencia": "toma",
    #     "numero_conta": "toma",
    #     "digito_verificador": "toma",
    #     "valor_transacao_financeira": "toma",
    #     "data_contabilizacao": "toma",
    #     "codigo_identificador_cliente": "toma"
    # }

    sm_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
    avro_str = AvroService().get_avro_schema(sm_client, 'pagamentos-pix-recebido-value')
    logging.info(avro_str)
    s_conf = {'auto.register.schemas': False, 'subject.name.strategy': lo_xap}
    serializer = AvroSerializer(
        schema_registry_client=sm_client,
        schema_str=avro_str,
        to_dict=serialize_event,
        conf=s_conf)
    avro_dict = json.loads(avro_str)
    evento_gerado = generateData(avro_dict)
    logging.info(evento_gerado)
    evento = serializer(evento_gerado, None)
    logging.info(f'evento em bytes: {evento}')
    deserializer = AvroDeserializer(sm_client)
    evento_na_estica = deserializer(evento, None)
    logging.info(f'evento normal: {evento_na_estica}')
