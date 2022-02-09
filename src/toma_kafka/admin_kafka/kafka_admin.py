from confluent_kafka.admin import AdminClient
import logging
from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_INVALID


class KafkaAdmin:
    def __init__(self) -> None:
        self.kafka_conf = {'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'consumer-kafka-pix'}


    def list_topics(self, topic):
        admin = Consumer(self.kafka_conf)
        topics = admin.list_topics(topic=topic).topics
        for k, v in topics.items():
            print(f'topic: {k} - partitions:{len(v.partitions)}')
    
    def toma_lag(self, topic):
        consumer = Consumer(self.kafka_conf)
        metadata = consumer.list_topics(topic, timeout=10)
        if metadata.topics[topic].error is not None:
            raise KafkaException(metadata.topics[topic].error)
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        
        committed = consumer.committed(partitions, timeout=10)

        for partition in committed:
            (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
            if hi < 0:
                lag = "no hwmark"  # Unlikely
            elif partition.offset < 0:
                # No committed offset, show total message count as lag.
                # The actual message count may be lower due to compaction
                # and record deletions.
                lag = "%d" % (hi - lo)
            else:
                lag = "%d" % (hi - partition.offset)

            print("%-50s offset: %9s lag: %9s" % ("Topic: {} [{}]".format(partition.topic, partition.partition), partition.offset, lag))

admin = KafkaAdmin()
admin.toma_lag('pagamentos-pix-emitido')
