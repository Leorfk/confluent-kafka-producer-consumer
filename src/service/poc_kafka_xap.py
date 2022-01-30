from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor

def consumir(topico):
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(topico,
                            group_id='consumer-kafka-pix',
                            bootstrap_servers=['localhost:9092'],
                            partition_assignment_strategy=[RoundRobinPartitionAssignor])
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
        #                                     message.offset, message.key,
        #                                     message.value))
        print(message.value)