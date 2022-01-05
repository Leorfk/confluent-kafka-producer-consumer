from service import kafka_confluent_producer, kafka_confluent_consumer, kafka_admin
from util import logging_config
import logging
from service.avro_service import AvroService
logging_config.init_logs(logging.INFO)

is_kafka_in_use = True
if is_kafka_in_use:
    # kafka_confluent_producer.gerar_massa('pagamentos-pix-emitido')
    # kafka_confluent_consumer.consume(['pagamentos-pix-emitido'])
    logging.info(AvroService().get_avro_schema())
    kafka_admin.list_topics()
