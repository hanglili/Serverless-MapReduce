import boto3
import json
import os
import decimal
import time

from static.static_variables import StaticVariables


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class InDegree:

    def __init__(self, in_lambda, is_local_testing):
        if is_local_testing:
            if in_lambda:
                local_endpoint_url = 'http://%s:4569' % os.environ['LOCALSTACK_HOSTNAME']
            else:
                local_endpoint_url = 'http://localhost:4569'
            self.client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',
                                        region_name=StaticVariables.DEFAULT_REGION,
                                        endpoint_url=local_endpoint_url)
        else:
            self.client = boto3.client('dynamodb')

    def create_in_degree_table(self, table_name):
        waiter = self.client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        self.client.create_table(
            AttributeDefinitions=[{
                'AttributeName': 'pipeline_id',
                'AttributeType': 'N'
            }],
            TableName=table_name,
            KeySchema=[{
                'AttributeName': 'pipeline_id',
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

        print("In degree table created successfully")

    def initialise_in_degree_table(self, table_name, in_degrees):
        for pipeline_id, in_degree in in_degrees.items():
            self.client.put_item(
                TableName=table_name,
                Item={
                    'pipeline_id': {'N': str(pipeline_id)},
                    'in_degree': {'N': str(in_degree)}
                }
            )
        print("In degree table initialised successfully")

    def read_in_degree_table(self, table_name):
        in_degrees = {}
        projection_expression = "pipeline_id, in_degree"
        response = self.client.scan(TableName=table_name, ProjectionExpression=projection_expression)
        for record in response['Items']:
            in_degrees[record['pipeline_id']['N']] = record['in_degree']['N']
        return in_degrees

    def delete_in_degree_table(self, table_name):
        existing_tables = self.client.list_tables()['TableNames']

        if table_name in existing_tables:
            self.client.delete_table(
                TableName=table_name
            )

            print("In degree table deleted successfully")
            return

        print("In degree table has not been created")

    def decrement_in_degree_table(self, table_name, pipeline_id):
        response = self.client.update_item(
            TableName=table_name,
            Key={
                'pipeline_id': {'N': str(pipeline_id)}
            },
            UpdateExpression="set in_degree = in_degree - :val",
            ExpressionAttributeValues={
                ':val': {'N': str(1)}
            },
            ReturnValues="UPDATED_NEW"
        )

        print("In-degree for pipeline %s incremented successfully" % pipeline_id)
        return response
