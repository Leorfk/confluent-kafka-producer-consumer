import boto3
import json


class LambdaXap:
    lambda_client = boto3.client("lambda", 'sa-east-1')

    def get_attachment(self, my_id):
        payload = {"myId": my_id}
        response = self.lambda_client.invoke(
            FunctionName='mamada',
            Payload=json.dumps(payload),
        )
        return response.status_code