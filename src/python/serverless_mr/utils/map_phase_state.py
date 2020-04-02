import boto3
import json
import os
import decimal
import time

from serverless_mr.static.static_variables import StaticVariables


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class MapPhaseState:
    DUMMY_ID = 0

    def __init__(self, in_lambda):
        self.static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
        if self.static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
            if in_lambda:
                local_endpoint_url = 'http://%s:4569' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4569'
            self.client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
        else:
            self.client = boto3.client('dynamodb')

    def create_state_table(self, table_name):
        self.client.create_table(
            AttributeDefinitions=[{
                'AttributeName': 'id',
                'AttributeType': 'N'
            }],
            TableName=table_name,
            KeySchema=[{
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        # Wait until the created table becomes active
        response = self.client.describe_table(TableName=table_name)['Table']['TableStatus']
        while response != 'ACTIVE':
            time.sleep(1)
            response = self.client.describe_table(TableName=table_name)['Table']['TableStatus']

        print("Map Phase state table created successfully")

    def initialise_state_table(self, table_name):
        self.client.put_item(
            TableName=table_name,
            Item={
                'id': {'N': str(MapPhaseState.DUMMY_ID)},
                'num_completed_mappers': {'N': str(0)}
            }
        )

        print("Map Phase state table initialised successfully")

    def delete_state_table(self, table_name):
        self.client.delete_table(
            TableName=table_name
        )

        print("Map Phase state table deleted successfully")

    def increment_num_completed_mapper(self, table_name):
        response = self.client.update_item(
            TableName=table_name,
            Key={
                'id': {'N': str(MapPhaseState.DUMMY_ID)}
            },
            UpdateExpression="set num_completed_mappers = num_completed_mappers + :val",
            ExpressionAttributeValues={
                ':val': {'N': str(1)}
            },
            ReturnValues="UPDATED_NEW"
        )

        print("Number of completed mappers incremented successfully")
        return response
