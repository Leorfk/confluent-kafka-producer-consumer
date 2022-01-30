from email.policy import default
from lorem.text import TextLorem
from functools import reduce
from random import randint, random, getrandbits, choice
import sys

lorem = TextLorem(srange=(2, 3))
lorem_key = TextLorem(srange=(1, 1))

default_tipo_generator = {
    'null': lambda _: None,
    'boolean': lambda _: bool(random()),
    'int': lambda _: getrandbits(32),
    'long': lambda _: getrandbits(64),
    'float': lambda _: random(),
    'double': lambda _: random(),
    'bytes': lambda _: bytes(getrandbits(2)),
    'string': lambda _: lorem.sentence(),
    ## complex types ##
    'enum': lambda field: choice(field['symbols']),
    'array': lambda field: gerador_tipo_array(field),
    'map': lambda field: gerador_tipo_map(field),
    'record': lambda field: generateData(field)
}


def pega_tipo_nao_nulo(tipos):
    return next(filter(lambda x: x != "null", tipos))


def gerador_tipo_map(field):
    if isinstance(field['items'], dict):
        return dict([(lorem_key.sentence(), generateData(field['items'])) for i in range(3)])
    else:
        return dict([(lorem_key.sentence(), gerador_por_tipo(field['items'])) for i in range(3)])


def gerador_tipo_array(field):
    if isinstance(field['items'], dict):
        return [generateData(field['items']) for i in range(3)]
    else:
        return [gerador_por_tipo(field['items']) for i in range(3)]


def gerador_por_tipo(tipo, field=None):
    if isinstance(tipo, dict):
        array_config = tipo['type']
        return default_tipo_generator[array_config](tipo)
    return default_tipo_generator[tipo](field)


def fields_iterator(record, field):
    name = field['name']
    tipo = field['type']
    if isinstance(tipo, list):
        tipo = pega_tipo_nao_nulo(tipo)
    
    record[name] = gerador_por_tipo(tipo, field)
    return record


def generateData(avroSchema):
    fields = avroSchema['fields']

    return reduce(fields_iterator, fields, {})
