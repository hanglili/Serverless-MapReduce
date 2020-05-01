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


class StageState:

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

    def create_state_table(self, table_name):
        waiter = self.client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        self.client.create_table(
            AttributeDefinitions=[{
                'AttributeName': 'stage_id',
                'AttributeType': 'N'
            }],
            TableName=table_name,
            KeySchema=[{
                'AttributeName': 'stage_id',
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

        print("Stage state table created successfully")

    def initialise_state_table(self, table_name, num_stages):
        for i in range(1, num_stages):
            self.client.put_item(
                TableName=table_name,
                Item={
                    'stage_id': {'N': str(i)},
                    'num_completed_operators': {'N': str(0)}
                }
            )

        # stage_id of -1 stores the current stage id
        # self.client.put_item(
        #     TableName=table_name,
        #     Item={
        #         'stage_id': {'N': str(-1)},
        #         'current_stage_id': {'N': str(1)}
        #     }
        # )
        print("Stage state table initialised successfully")

    def read_state_table(self, table_name):
        stage_states = {}
        projection_expression = "stage_id, num_completed_operators"
        response = self.client.scan(TableName=table_name, ProjectionExpression=projection_expression)
        for record in response['Items']:
            stage_states[record['stage_id']['N']] = record['num_completed_operators']['N']
        return stage_states

    def delete_state_table(self, table_name):
        existing_tables = self.client.list_tables()['TableNames']

        if table_name in existing_tables:
            self.client.delete_table(
                TableName=table_name
            )

            print("Stage state table deleted successfully")
            return

        print("Stage state table has not been created")

    def increment_num_completed_operators(self, table_name, stage_id):
        response = self.client.update_item(
            TableName=table_name,
            Key={
                'stage_id': {'N': str(stage_id)}
            },
            UpdateExpression="set num_completed_operators = num_completed_operators + :val",
            ExpressionAttributeValues={
                ':val': {'N': str(1)}
            },
            ReturnValues="UPDATED_NEW"
        )

        print("Number of completed operators incremented successfully")
        return response

    def increment_current_stage_id(self, table_name):
        response = self.client.update_item(
            TableName=table_name,
            Key={
                'stage_id': {'N': str(-1)}
            },
            UpdateExpression="set current_stage_id = current_stage_id + :val",
            ExpressionAttributeValues={
                ':val': {'N': str(1)}
            },
            ReturnValues="UPDATED_NEW"
        )

        print("Current stage id incremented successfully")
        return response

    def read_current_stage_id(self, table_name):
        projection_expression = "current_stage_id"
        response = self.client.get_item(TableName=table_name,
                                        Key={
                                            'stage_id': { 'N': str(-1)}
                                        },
                                        ProjectionExpression=projection_expression)
        return int(response['Item']['current_stage_id']['N'])
