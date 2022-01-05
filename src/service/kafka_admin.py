from confluent_kafka.admin import AdminClient, TopicMetadata
import logging
from util import logging_config
logging_config.init_logs(logging.INFO)
kafka_conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'consumer-kafka-pix',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

def list_topics():
    admin = AdminClient(kafka_conf)
    topics = admin.list_topics().topics
    for k, v in topics.items():
        logging.info(f'topic: {k} - partitions:{len(v.partitions)}')
