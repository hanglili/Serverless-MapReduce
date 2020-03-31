import boto3
import json
import resource
import time
import os

from serverless_mr.static.static_variables import StaticVariables

# create an S3 & Dynamo session
static_job_info = json.loads(open(StaticVariables.STATIC_JOB_INFO_PATH, 'r').read())
if static_job_info[StaticVariables.LOCAL_TESTING_FLAG_FN]:
    s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
                             region_name=StaticVariables.DEFAULT_REGION,
                             endpoint_url='http://%s:4572' % os.environ['LOCALSTACK_HOSTNAME'])
    dynamodb_client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',
                                   region_name=StaticVariables.DEFAULT_REGION,
                                   endpoint_url='http://%s:4569' % os.environ['LOCALSTACK_HOSTNAME'])
else:
    s3_client = boto3.client('s3')
    dynamodb_client = boto3.client('dynamodb')


def write_to_s3(bucket, key, data, metadata):
    # Write to S3 Bucket
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)


def write_to_dynamodb(key, data, metadata):
    def create_dynamo_table(client, table_name):
        try:
            response = client.create_table(
                AttributeDefinitions=[{
                    'AttributeName': 'id',
                    'AttributeType': 'S'
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
        except client.exceptions.ResourceInUseException as e:
            print("Metadata table already created.")
        # print(response)
        # print(json.dumps(response))

    def put_items(client, table_name, data):
        for pair in data:
            response = client.put_item(
                TableName=table_name,
                Item={
                    'id': {'S': str(pair[0])},
                    'line': {'S': str(pair[1])}
                }
            )

    def put_metadata(client, table_name, metadata):
        response = client.put_item(
            TableName=table_name,
            Item={
                'id': {'S': str(key)},
                'metadata': {'S': str(metadata)}
            }
        )

    # Write to Dynamo DB
    dynamodb_table_name = "metadata"
    create_dynamo_table(dynamodb_client, key)
    create_dynamo_table(dynamodb_client, dynamodb_table_name)
    put_items(dynamodb_client, key, data)
    put_metadata(dynamodb_client, dynamodb_table_name, metadata)


def reduce_handler(reduce_function):
    def lambda_handler(event, _):

        start_time = time.time()

        job_bucket = event['jobBucket']
        # bucket = event['bucket']
        reduce_keys = event['keys']
        job_id = event['jobId']
        reducer_id = event['reducerId']
        # step_id = event['stepId']
        # n_reducers = event['numReducers']
        use_combine = event['useCombine']
        output_bucket = event['outputBucket']
        output_prefix = \
            "%s/%s" % (job_id, StaticVariables.REDUCE_OUTPUT_PREFIX) if event['outputPrefix'] == "" \
            else event['outputPrefix']

        # aggr
        line_count = 0
        intermediate_data = []

        # INPUT JSON => OUTPUT JSON

        # Download and process all keys
        for key in reduce_keys:
            response = s3_client.get_object(Bucket=job_bucket, Key=key)
            contents = response['Body'].read()

            for key_value in json.loads(contents):
                line_count += 1
                intermediate_data.append(key_value)

        intermediate_data.sort(key=lambda x: x[0])

        cur_key = None
        cur_values = []
        outputs = []
        for key, value in intermediate_data:
            if cur_key == key:
                if use_combine:
                    cur_values += value
                else:
                    cur_values.append(value)
            else:
                if cur_key is not None:
                    cur_key_outputs = []
                    reduce_function(cur_key_outputs, (cur_key, cur_values))
                    outputs += cur_key_outputs

                cur_key = key
                if use_combine:
                    cur_values = value
                else:
                    cur_values = [value]

        if cur_key is not None:
            cur_key_outputs = []
            reduce_function(cur_key_outputs, (cur_key, cur_values))
            outputs += cur_key_outputs

        time_in_secs = (time.time() - start_time)
        # timeTaken = time_in_secs * 1000000000 # in 10^9
        # s3DownloadTime = 0
        # totalProcessingTime = 0
        processing_info = [len(reduce_keys), line_count, time_in_secs]
        print("Reducer process information: (number of keys processed, line count, processing time)\n", processing_info)

        metadata = {
            "lineCount": '%s' % line_count,
            "processingTime": '%s' % time_in_secs,
            "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        }

        # filename = "%s%s" % (output_prefix, reducer_id)
        #
        # write_to_s3(output_bucket, filename, json.dumps(outputs), metadata)

        filename = "output-%s" % reducer_id

        write_to_dynamodb(filename, outputs, json.dumps(metadata))
        return processing_info

    lambda_handler.__wrapped__ = reduce_function
    return lambda_handler
