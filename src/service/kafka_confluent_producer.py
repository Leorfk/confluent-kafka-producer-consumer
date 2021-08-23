from confluent_kafka import Producer
import logging
from datetime import datetime

eventos = [
    {'nome': 'leonardo', 'idade': 26},
    {'nome': 'Rita', 'idade': 53},
    {'nome': 'João', 'idade': 28},
    {'nome': 'Thamiris', 'idade': 31},
    {'nome': 'Alfonsa', 'idade': 15}
]

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def gerar_massa():
    for c in range(10000):
        for data in eventos:
            # Trigger any available delivery report callbacks from previous produce() calls
            p.poll(0)
            data.update({'data_cadastro': str(datetime.now())})
            logging.info(data)
            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            p.produce('xap', str(data).encode('utf-8'), callback=delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
    p.flush()
