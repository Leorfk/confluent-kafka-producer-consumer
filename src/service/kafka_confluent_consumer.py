from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
import logging
from util import logging_config

logging_config.init_logs(logging.INFO)
kafka_conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
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

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            logging.error("Consumer error: {}".format(msg.error()))
            continue
        logging.info(f'''
        header: {msg.headers()},
        payload: {msg.value().decode('utf-8')}''')

    c.close()
