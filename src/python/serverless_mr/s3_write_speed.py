import boto3
import time
import json

client = boto3.client('s3')
start_time = time.time()
coordinator_execution_time = time.time() - start_time
coordinator_execution_info_s3_key = "%s" % "test"
coordinator_execution_info = {"processingTime": '%s' % str(12)}
client.put_object(Bucket="serverless-mapreduce-storage-query-13", Key=coordinator_execution_info_s3_key,
                  Body=json.dumps({}), Metadata=coordinator_execution_info)

print("The execution time is: %s" % str(time.time() - start_time))
