#coding: utf-8
from confluent_kafka import Producer
import logging
from repository.gerador_de_eventos import generate_event
from util import logging_config
logging_config.init_logs(logging.INFO)
p = Producer({'bootstrap.servers': '127.0.0.1:9092'})


def delivery_report(err, msg):
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def gerar_massa(topics):
    for topic in topics:
        for c in range(300000):
            p.poll(0)
            header = {'toma': topic}
            p.produce(topic=topic,
                      value=str(generate_event()).encode('utf-8'),
                      callback=delivery_report,
                      headers=header)
    p.flush()


if __name__ == '__main__':
    t = ['LOJA_NOVO_PEDIDO','topico.comando.teste','ECOMMERCE_SEND_EMAIL','xap','ECOMMERCE_NEW_ORDER','kafka-python-topic']
    gerar_massa(t)
