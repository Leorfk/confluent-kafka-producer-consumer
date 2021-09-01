# coding: utf-8
from confluent_kafka import Producer, KafkaError
import logging
from repository.gerador_de_eventos import generate_event, generate_headers
from util import logging_config
from multiprocessing import Process

logging_config.init_logs(logging.INFO)
kafka_config = {'bootstrap.servers': '127.0.0.1:9092'}
p = Producer(kafka_config)


def delivery_report(err, msg):
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(),
                     msg.partition()))


def gerar_massa(topics):
    for topic in topics:
        try:
            for c in range(300000):
                p.poll(0)
                p.produce(topic=topic,
                          value=str(generate_event()).encode('utf-8'),
                          callback=delivery_report,
                          headers=generate_headers())
        except (KafkaError, BufferError) as error:
            logging.error(f'Deu pau no producer {topic}, motivo: {error}')
    p.flush()


if __name__ == '__main__':
    t = ['LOJA_NOVO_PEDIDO', 'topico.comando.teste', 'ECOMMERCE_SEND_EMAIL',
         'xap', 'ECOMMERCE_NEW_ORDER', 'kafka-python-topic']
    Process(target=gerar_massa, args=(t,)).start()
    #gerar_massa(t)
