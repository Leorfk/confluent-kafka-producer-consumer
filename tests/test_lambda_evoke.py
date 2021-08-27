import unittest
from src.service import lambda_evoke


class TestLambdaEvoke(unittest.TestCase):

    def setUp(self):
        self.test_lambda = lambda_evoke.LambdaEvoke()

    def test_lambda_evoke_somar_menor_que_dez(self):
        assert self.test_lambda.somar(7, 1) == 8

    def test_lambda_evoke_somar_maior_que_dez(self):
        assert self.test_lambda.somar(7, 11) == 0
