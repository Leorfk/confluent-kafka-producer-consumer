from datetime import datetime
from random import randint
from uuid import uuid4
import random
names = ['PETTER PAN', 'HITMAN', 'BANANA', 'MACACO LOUCO', 'LUFFY', 'ZORO', 'CHINPANZE ALBINO', 'AMY', 'ALA']


def generate_event():
    return {
        'nome': random.choice(names),
        'data_criacao': str(datetime.today()),
        'cpf_cnpj_cliente': str(randint(0, 99999999999)),
        'rg': str(randint(0, 999999999)),
        'valor_lancamento': round(randint(0, 1000000) / 3, 2)
    }


def generate_headers():
    return {
        'id': str(uuid4()),
        'time': str(datetime.today()),
        'transactionid': str(uuid4())
    }
