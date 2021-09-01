from datetime import datetime
from random import randint
from uuid import uuid4
import random
names = ['xap', 'toma', 'deita', 'alou', 'haha', 'rhtrg', 'egrhtjer4f', 'werterfef', 'wegrerfgfqw']


def generate_event():
    return {
        'nome': random.choice(names),
        'data': str(datetime.today()),
        'CPF': randint(0, 99999999999),
        'rg': randint(0, 999999999),
        'valor_lancamento': round(randint(0, 1000000) / 3, 2)
    }


def generate_headers():
    return {
        'id': str(uuid4()),
        'time': str(datetime.today()),
        'transactionid': str(uuid4())
    }
