# Kafka Producer/Consumer

## O QUE É
Implementação de um consumer e um producer kafka

## A QUEM SE DESTINA / OBJETIVO
Um projeto simples que tem como finalidade a consolidação de conhecimentos na implementação de consumidores e produtores kafka

## INSTALAÇÃO LOCAL
### Configuração Kafka
- Realize a instalalção do Apache Kafka e Zookeeper.
- Após instalar o kafka, vá até o diretório *kafka_`versão do kafka` e execute o seguinte comando

```bash
bin/kafka-server-start.sh config/server.properties
```

- Para criar um tópico execute o seguinte comando
```bash
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic <nome do topico>
```

### Configuração do projeto python
- Para executar o projeto, primeiro execute o seguinte comando no diretório raiz do projeto

```bash
pip install -r requirements.txt
```
- Para executar o producer kafka execute o seguinte comando
```bash
python3 src/service/kafka_confluent_producer.py
```
- Para executar o consumer kafka execute o seguinte comando
```bash
python3 src/service/kafka_confluent_consumer.py
```


### BIBLIOTECAS DO PROJETO
- Usamos o [ Apache Kafka ](https://kafka.apache.org/) na versão 2.12-2.8.0
Biblioteca [ confluent_kafka ](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#).

---
### SOBRE O AUTOR/ORGANIZADOR
Leonardo Rodrigues Ferreira
leonardorodrigues_f@hotmail.com
