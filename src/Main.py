from service import kafka_confluent_producer, kafka_confluent_consumer
from util import logging_config
import logging
logging_config.init_logs(logging.INFO)

is_kafka_in_use = True
if is_kafka_in_use:
    kafka_confluent_producer.gerar_massa()
    kafka_confluent_consumer.consume()
