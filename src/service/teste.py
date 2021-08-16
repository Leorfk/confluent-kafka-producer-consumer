from kafka import KafkaConsumer

topic = 'xap'
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['127.0.0.1:9092'],
    client_id = 'CLI',
    group_id='GR',
    auto_offset_reset='earliest',
    consumer_timeout_ms=10000)

print(len(consumer.partitions_for_topic(topic)))