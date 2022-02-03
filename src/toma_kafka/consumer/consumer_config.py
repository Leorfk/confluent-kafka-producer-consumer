class ConsumerConfig:
    def __init__(self) -> None:
        pass

    def get_consumer_settings(self, custom_config=None):
        config = {
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'consumer-kafka-pix',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'partition.assignment.strategy': 'cooperative-sticky'
        }
        if custom_config:
            config.update(custom_config)
        return config
