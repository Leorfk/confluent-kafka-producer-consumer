from uuid import uuid4
from confluent_kafka import Producer


class ProducerGeneric:
    def __init__(self, producer_config) -> None:
        self.__producer = Producer(producer_config)
        pass

    def send_message(self, topico, evento, header, callback=None, message_id=str(uuid4())):
        if callback is None:
            callback = self.callback
        self.__producer.produce(topic=topico, key=message_id,
                                value=evento, headers=header, on_delivery=callback)
        self.__producer.flush()

    def callback(self, error, msg):
        if error:
            print({'mensagem': 'pau na mensagem',
                           'key': msg.key(), 'erro': error})
        else:
            print({'mensagem': 'mensagem produzida com sucesso',
                          'patition': msg.partition(),
                          'offset': msg.offset(),
                          'key': msg.key()})
