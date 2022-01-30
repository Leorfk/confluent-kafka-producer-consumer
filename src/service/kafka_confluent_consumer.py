from confluent_kafka import Consumer, KafkaException
from confluent_kafka.avro import AvroConsumer
import logging
from multiprocessing import Process
from util import logging_config
logging_config.init_logs(logging.DEBUG)
from datetime import datetime

kafka_conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'consumer-kafka-pix',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'partition.assignment.strategy': 'cooperative-sticky'
}


def consume(topic):
    # kafka_conf.update({'debug': 'broker, cgrp'})
    c = Consumer(kafka_conf)
    c.subscribe(topic)
    logging.info('Começou!!!')
    contador = 0
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            logging.error("Consumer error: {}".format(msg.error()))
            continue
        logging.info(f'''header: {msg.headers()},payload: {msg.value().decode('utf-8')}''')
        # c.commit()


def eventos_na_estica(topic):
    #kafka_conf.update({'debug': 'broker, cgrp'})
    c = Consumer(kafka_conf)
    c.subscribe(topic)
    #logging.info(f'Começou o xap no tópico {topic}')
    while True:
        try:
            msgs = c.consume(num_messages=2000, timeout=1)
            if not msgs:
                # logging.info(f'tô no aguardo {topic}')
                # logging.info(msgs)
                continue
            data_inicio = datetime.today()
            # logging.info('recebi')
            # logging.info(msgs)
            # logging.info(f'tamanho do batch {len(msgs)}')
            for msg in msgs:
            #     #logging.info(f'''header: {msg.headers()},payload: {msg.value().decode('utf-8')}''')
                c.commit(msg)
            logging.info(f'Tópico: {topic} | Tamnho do lote: {len(msgs)} | Tempo de consumo: {abs(data_inicio - datetime.today())}')
            # 2k de mensagem em batch 0:00:00.001451
        except KafkaException as ex:
            logging.error(ex)


# if __name__ == '__main__':
#     topicos = ['pix-conciliacao', 'pix-devolucao-emitida', 'pix-devolucao-recebida', 'pix-emitido', 'pix-recebido']
#     #eventos_na_estica(['pix-conciliacao'])
#     for topic in topicos:
#         logging.info(topic)
#         Process(target=eventos_na_estica, args=([topic],)).start()
