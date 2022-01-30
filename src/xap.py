import json
import logging
HEADER_KEYS = ['id', 'correlationid', 'time', 'type', 'transactionid']


class Message:
    payload = {
        'id': 'toma',
        'data':
        {
            'valor_transacao': 2000.98,
            'data_lancamento': '2022-10-30 01:30:09',
            'codigo_clinete': 'jamal'
        }
    }
    header = [
        ('id', b'xap'),
        # ('transactionid', b''),
        ('correlationid', b'xablau'),
        ('time', b'toma'),
        ('type', b'toma')
    ]
    error = None

    def headers(self):
        return self.header

    def errors(self):
        return self.error

    def value(self):
        return self.payload


def kafka_header_to_dict(header: list):
    return {k: v.decode('utf-8') if v else None for k, v in dict(header).items()}


def join_payload_with_header(message: Message):
    header = kafka_header_to_dict(message.headers())
    payload = message.value()
    payload.update(header)
    return payload


def verify_header_layout(event: dict):
    header_received = {
        key: value for key, value in event.items() for k in HEADER_KEYS if key == k}
    if len(header_received) == len(HEADER_KEYS):
        verify_header_values(header_received)
    else:
        raise KeyError(json.dumps({
            'mensagem': 'evento mal formatado',
            'chaves_esperadas': HEADER_KEYS,
            'chaves_recebidas': header_received}))


def verify_header_values(header):
    header_value_none = {k: v for k,
                         v in header.items() if not v}
    if header_value_none:
        raise KeyError(json.dumps({
            'mensagem': 'identificador nulo',
            'chaves_problematicas': header_value_none
        }))

try:
    event = join_payload_with_header(Message())
    verify_header_layout(event)
except Exception as e:
    logging.error(e)
