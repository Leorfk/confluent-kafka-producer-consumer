import unittest
from unittest.mock import patch, Mock
from lambda_gulosa import LambdaXap


class LambdaTest(unittest.TestCase):

    @patch("lambda_gulosa.LambdaXap.lambda_client")
    def test_duck_response_200(self, mock_lambda_client):
        mocked_response = Mock()
        mocked_response.status_code = 200
        mock_lambda_client.invoke.return_value = mocked_response
        response = LambdaXap().get_attachment('some_id')
        assert response == 200
