from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
import logging
from multiprocessing import Process
from util import logging_config
logging_config.init_logs(logging.INFO)

def list_topics():
    kafka_conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }
    admin = AdminClient(kafka_conf)
    topics = admin.list_topics().topics
    logging.info(topics)
    for k, v in topics.items():
        logging.info(f'{k} - {len(v.partitions)}')


def consume(topic):
    kafka_conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }
    c = Consumer(kafka_conf)
    c.subscribe([topic])
    logging.info('Começou!!!')
    contador = 0
    while True:
        msg = c.poll(timeout=1)

        if msg is None:
            contador += 1
            logging.warning(f'Tópico {topic} passou {contador} vezes sem mensagem')
            if contador == 10:
                logging.warning('COnsumer será desligado, pois não existem mais eventos a serem consumidos')
                continue
            else:
                continue
        if msg.error():
            logging.error("Consumer error: {}".format(msg.error()))
            continue
        logging.info(f'''header: {msg.headers()},payload: {msg.value()
        .decode('utf-8')}''')
        c.commit()


def eventos_na_estica(topic):
    kafka_conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }
    c = Consumer(kafka_conf)
    c.subscribe([topic])
    logging.info(f'Começou o xap no tópico {topic}')
    while True:
        logging.info(f'tô no aguardo {topic}')
        msgs = c.consume(5000)
        if not msgs:
            logging.info('Sem nada aqui')
            continue
        try:
            for msg in msgs:
                logging.info(f'''header: {msg.headers()},payload: 
{msg.value().decode('utf-8')}''')
                c.commit(msg)

        except KafkaException as ex:
            logging.error(ex)


if __name__ == '__main__':
    topics = ['LOJA_NOVO_PEDIDO', 'topico.comando.teste',
              'ECOMMERCE_SEND_EMAIL', 'xap', 'ECOMMERCE_NEW_ORDER',
              'kafka-python-topic']
    for topic in topics:
        logging.info(topic)
        Process(target=eventos_na_estica, args=(topic,)).start()
        logging.info(f'passamo o famoso: {topic}')
