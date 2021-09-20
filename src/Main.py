from service import kafka_confluent_producer, kafka_confluent_consumer
from util import logging_config
import logging
from datetime import datetime
logging_config.init_logs(logging.INFO)

# is_kafka_in_use = True
# if is_kafka_in_use:
#     kafka_confluent_producer.gerar_massa()
#     kafka_confluent_consumer.consume()
dif = '2021-09-18 22:12:21.929013', '2021-09-18 22:12:23.251758'
def diff_days(date1, date2):
    d1 = datetime.strptime(date1, "%Y-%m-%d %H:%M:%S.%f")
    d2 = datetime.strptime(date2, "%Y-%m-%d %H:%M:%S.%f")
    return abs(d1 - d2)

print(f"teste com 1000: {diff_days('2021-09-18 22:12:21.929013','2021-09-18 22:12:23.251758')}")
print(f"teste com 2000: {diff_days('2021-09-18 22:23:53.676231','2021-09-18 22:23:56.251451')}")
print(f"teste com 2000 em 0.0001 segundos: {diff_days('2021-09-18 22:27:51.724700','2021-09-18 22:27:51.927231')}")
