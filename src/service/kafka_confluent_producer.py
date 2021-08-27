#coding: utf-8
from confluent_kafka import Producer
import logging
from repository.gerador_de_eventos import generate_event

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})


def delivery_report(err, msg):
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def gerar_massa():
    for c in range(500000):
        p.poll(0)
        p.produce(topic='xap',
                  value=str(generate_event()).encode('utf-8'),
                  callback=delivery_report,
                  headers={"toma": "xap"})
    p.flush()


gerar_massa()
