from kafka import KafkaProducer
from datetime import datetime
import logging
from util import logging_config

logging_config.init_logs(logging.INFO)
producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')


def produce():
    while True:
        mensagem = str.encode('Mensagem {0}'.format(datetime.now()))
        logging.info(mensagem)
        producer.send('xap', mensagem)
