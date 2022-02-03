from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


class ProducerConfig:

    def __init__(self, sm_client: SchemaRegistryClient) -> None:
        self.sm_client = sm_client

    def __serialize_message(self, message: dict, ctx):
        return message

    def __get_schema_str(self, subject_name):
        return self.sm_client.get_latest_version(subject_name).schema.schema_str

    def __value_avro_serializer(self, subject_name):
        serialize_config = {'auto.register.schemas': False,
                            'subject.name.strategy': topic_subject_name_strategy}
        return AvroSerializer(self.sm_client, self.__get_schema_str(subject_name), self.__serialize_message, serialize_config)

    def __key_serializer_str(self):
        return StringSerializer('utf_8')

    def get_serializing_producer_configs(self, kafka_broker: str, subject_name, custom_configs=None):
        config = {
            'bootstrap.servers': kafka_broker,
            'key.serializer': self.__key_serializer_str(),
            'value.serializer': self.__value_avro_serializer(subject_name)}
        if custom_configs:
            config.update(custom_configs)
        return config

    def get_generic_producer_configs(self, kafka_broker: str, custom_configs=None):
        config = {'bootstrap.servers': kafka_broker}
        if custom_configs:
            config.update(custom_configs)
        return config
