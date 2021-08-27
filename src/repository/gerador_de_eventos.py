from datetime import datetime
from random import randint
import random
names = ['xap', 'toma', 'deita', 'alou', 'haha', 'rhtrg', 'egrhtjer4f', 'werterfef', 'wegrerfgfqw']


def generate_event():
    return {
        'nome': random.choice(names),
        'data': str(datetime.today()),
        'CPF': randint(0, 1000000),
        'rg': randint(0, 1000000),
        'valor_lancamento': round(randint(0, 1000000) / 3, 2)
    }
