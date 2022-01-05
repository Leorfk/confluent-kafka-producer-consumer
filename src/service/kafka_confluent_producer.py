# coding: utf-8
from confluent_kafka import Producer, KafkaError
import logging
from repository.gerador_de_eventos import generate_event, generate_headers
from util import logging_config
from multiprocessing import Process
from datetime import datetime
logging_config.init_logs(logging.INFO)
kafka_config = {'bootstrap.servers': '127.0.0.1:9092'}
p = Producer(kafka_config)


def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    # else:
        # logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def gerar_massa(topic):
    try:
        inicio_processo = datetime.today()
        logging.info('Gerando massa')
        for c in range(2000):
            p.produce(topic=topic,
                      value=str(generate_event()).encode('utf-8'),
                      callback=delivery_report,
                      headers=generate_headers())
            p.poll(0.1)
            p.flush()
        fim_processo = datetime.today()
        logging.info(f'{topic} - {diff_days(inicio_processo, fim_processo)}')
    except (KafkaError, BufferError) as error:
        logging.error(f'Deu pau no producer {topic}, motivo: {error}')

def gerar_massa_multi(topic):
    try:
        p = Producer(kafka_config)
        inicio_processo = datetime.today()
        for c in range(1000):
            p.produce(topic=topic,
                      value=str(generate_event()).encode('utf-8'),
                      callback=delivery_report,
                      headers=generate_headers())
            p.poll(0.1)
            p.flush()
        fim_processo = datetime.today()
        logging.info(f'{topic} - {diff_days(inicio_processo, fim_processo)}')
    except (KafkaError, BufferError) as error:
        logging.error(f'Deu pau no producer {topic}, motivo: {error}')


def diff_days(date1, date2):
    # d1 = datetime.strptime(date1, "%Y-%m-%d %H:%M:%S.%f")
    # d2 = datetime.strptime(date2, "%Y-%m-%d %H:%M:%S.%f")
    return abs(date1 - date2)

# if __name__ == '__main__':
#     topicos = ['pix-conciliacao', 'pix-devolucao-emitida', 'pix-devolucao-recebida', 'pix-emitido', 'pix-recebido']
#     # gerar_massa('pix-conciliacao')
#     for topico in topicos:
#         #gerar_massa_multi(topico)
#         for c in range(10):
#             Process(target=gerar_massa_multi, args=(topico,)).start()
#         logging.info(f'iniciando o producer para o tópico: {topico}')
