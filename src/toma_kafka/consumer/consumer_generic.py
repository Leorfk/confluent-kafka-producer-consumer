from confluent_kafka import Consumer

class ConsumerGeneric:
    def __init__(self, consumer_config) -> None:
        self.consumer_config = consumer_config
    
    def create_consumer(self):
        return Consumer(self.consumer_config)
