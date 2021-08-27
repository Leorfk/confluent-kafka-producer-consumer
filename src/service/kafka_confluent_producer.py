#coding: utf-8
from confluent_kafka import Producer
import logging
from repository.gerador_de_eventos import generate_event

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def gerar_massa():
    for c in range(500000):
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)
        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        p.produce(topic='xap',
                  value=str(generate_event()).encode('utf-8'),
                  callback=delivery_report, headers={"toma": "xap"})

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
    p.flush()

gerar_massa()