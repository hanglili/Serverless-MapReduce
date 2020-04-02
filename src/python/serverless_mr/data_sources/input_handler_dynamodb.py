import boto3
import json
import os
import time

from serverless_mr.static.static_variables import StaticVariables


class InputHandlerDynamoDB:

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

    @staticmethod
    def create_table(client, table_name, input_key_name):
        client.create_table(
            AttributeDefinitions=[{
            'AttributeName': input_key_name,
            'AttributeType': 'S'
            }],
            TableName=table_name,
            KeySchema=[{
            'AttributeName': input_key_name,
            'KeyType': 'HASH'
            }],
            ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
            }
        )

        # Wait until the created table becomes active

        response = client.describe_table(TableName=table_name)['Table']['TableStatus']
        while response != 'ACTIVE':
            time.sleep(1)
            response = client.describe_table(TableName=table_name)['Table']['TableStatus']


    @staticmethod
    def put_items(client, table_name, filepath, input_key_name, input_column_name):
        with open(filepath) as fp:
            line = fp.readline()
            id_cnt = 1
            while line:
                response = client.put_item(
                    TableName=table_name,
                    Item={
                        input_key_name: {'S': str(id_cnt)},
                        input_column_name: {'S': line.strip()}
                    }
                )
                line = fp.readline()
                id_cnt += 1

    # DynamoDB table is config["bucket"]?
    def set_up_local_input_data(self, input_file_paths):
        prefix = self.static_job_info[StaticVariables.INPUT_PREFIX_FN]
        input_key_name = self.static_job_info[StaticVariables.INPUT_KEY_NAME_DYNAMODB]
        input_column_name = self.static_job_info[StaticVariables.INPUT_COLUMN_NAME_DYNAMODB]
        for i in range(len(input_file_paths)):
            input_filepath = input_file_paths[i]
            table_name = '%s-input-%s' % (prefix, str(i + 1))
            InputHandlerDynamoDB.create_table(self.client, table_name, input_key_name)
            InputHandlerDynamoDB.put_items(self.client, table_name, input_filepath, input_key_name, input_column_name)

        print("Set up local input data successfully")

    def get_all_input_keys(self):
        # Returns all input keys to be processed: a list of format obj where obj is a map of {'Key': ..., 'Size': ...}
        all_keys = []
        prefix = self.static_job_info[StaticVariables.INPUT_PREFIX_FN]
        table_names = self.client.list_tables()['TableNames']
        for table_name in table_names:
            if table_name.startswith(prefix):
                response = self.client.describe_table(TableName=table_name)
                size = response['Table']['ItemCount']
                all_keys.append({'Key': table_name, 'Size': int(size)})

        return all_keys

    def read_records_from_input_key(self, input_key):
        lines = []
        input_column_name = self.static_job_info[StaticVariables.INPUT_COLUMN_NAME_DYNAMODB]
        response = self.client.scan(TableName=input_key, ProjectionExpression=input_column_name)
        for record in response['Items']:
            line = record[input_column_name]['S']
            lines.append(line)

        return lines
