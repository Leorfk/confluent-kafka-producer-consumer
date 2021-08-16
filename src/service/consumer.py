from kafka import KafkaConsumer
import json
topics = [
'ECOMMERCE_NEW_ORDER',
'ECOMMERCE_SEND_EMAIL',
'LOJA_NOVO_PEDIDO',
'xap'
]

consumer = KafkaConsumer(
    bootstrap_servers=["127.0.0.1:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer_da_gula",
    # sasl_mechanism="PLAIN",
    # sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    # sasl_plain_username="$ConnectionString",
    # security_protocol="SASL_SSL",
    # value_deserializer=lambda x: x.decode("utf-8")
    )

consumer.subscribe(topics)

print('Vamos começar')
for msg in consumer:
    print('-----------------------------------------------------')
    # message = json.loads(msg.value)
    print(msg)
    # print(type(message))