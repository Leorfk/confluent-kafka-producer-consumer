from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
import logging
from util import logging_config

logging_config.init_logs(logging.INFO)
kafka_conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

c = Consumer(kafka_conf)

c.subscribe(['xap'])


def list_topics():
    admin = AdminClient(kafka_conf)
    topics = admin.list_topics().topics
    logging.info(topics)
    for k, v in topics.items():
        logging.info(f'{k} - {len(v.partitions)}')


def consume():
    logging.info('Começou!!!')
    contador = 0
    while True:
        msg = c.poll(1.0)

        if msg is None:
            contador += 1
            logging.warning(contador)
            if contador == 10:
                logging.warning('COnsumer será desligado, pois não existem mais eventos a serem consumidos')
                c.close()
            else:
                continue
        if msg.error():
            logging.error("Consumer error: {}".format(msg.error()))
            continue
        logging.info(f'''
        header: {msg.headers()},
        payload: {msg.value().decode('utf-8')}''')


def toma():
    msg = None
    count = 0
    while msg is None:
        msg = c.poll(1000)
        logging.info(msg.value())
        logging.warning(type(msg))

def eventos_na_estica():
    logging.info('Começou!!!')
    file = open('xap.json', 'x')
    while True:
            logging.info('tô no aguardo')
            msgs = c.consume(50000)
            if not msgs:
                continue
            try:
                for msg in msgs:
                    # logging.info(f'''header: {msg.headers()},payload: {msg.value().decode('utf-8')}''')
                    file.write(f'''header: {msg.headers()},payload: {msg.value().decode('utf-8')}''' + '\n')
                    c.commit(msg)

            except KafkaException as ex:
                logging.error(ex)


eventos_na_estica()
