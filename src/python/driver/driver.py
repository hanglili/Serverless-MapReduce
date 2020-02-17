import boto3
import json
import random
import time

from utils import access_s3, data_encoding, lambda_utils
from aws_lambda import lambda_manager
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from botocore.client import Config


class Driver:
    JOB_ID = "bl-release"
    DRIVER_CONFIG_PATH = "configuration/driver.json"
    L_PREFIX = "BL"
    CODE_MAPPER_PREFIX = JOB_ID + "/code/mapper/"
    CODE_REDUCER_PREFIX = JOB_ID + "/code/reducer/"
    TASK_MAPPER_PREFIX = JOB_ID + "/task/mapper/"
    TASK_REDUCER_PREFIX = JOB_ID + "/task/reducer/"
    EXECUTOR_PATH = "executor/executor.py"
    EXECUTOR_ZIP_PATH = "executor.zip"
    EXECUTOR_HANDLER_PATH = "executor/executor.lambda_handler"

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.lambda_config = None
        self.lambda_client = None
        self.config = json.loads(open(Driver.DRIVER_CONFIG_PATH, 'r').read())

    # Get all keys to be processed
    def _get_all_keys(self):
        # init
        bucket = self.config["bucket"]
        region = self.config["region"]
        lambda_memory = self.config["lambdaMemory"]
        concurrent_lambdas = self.config["concurrentLambdas"]
        lambda_read_timeout = self.config["lambda_read_timeout"]
        boto_max_connections = self.config["boto_max_connections"]

        # Setting longer timeout for reading aws_lambda results and larger connections pool
        self.lambda_config = Config(read_timeout=lambda_read_timeout,
                                    max_pool_connections=boto_max_connections,
                                    region_name=region)
        self.lambda_client = boto3.client('lambda', config=self.lambda_config)

        # Fetch all the keys that match the prefix
        all_keys = []
        for obj in self.s3.Bucket(bucket).objects.filter(Prefix=self.config["prefix"]).all():
            all_keys.append(obj)

        bsize = lambda_utils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas)
        batches = lambda_utils.batch_creator(all_keys, bsize)
        num_mappers = len(batches)
        return all_keys, num_mappers, batches

    # Create the aws_lambda functions
    def _create_lambda(self, num_mappers):

        # Lambda functions
        mapper_lambda_name = Driver.L_PREFIX + "-mapper-" + Driver.JOB_ID
        reducer_lambda_name = Driver.L_PREFIX + "-reducer-" + Driver.JOB_ID
        executor_lambda_name = Driver.L_PREFIX + "-executor-" + Driver.JOB_ID
        rc_lambda_name = Driver.L_PREFIX + "-rc-" + Driver.JOB_ID
        job_bucket = self.config["jobBucket"]
        region = self.config["region"]
        num_reducers = self.config["num_reducers"]

        # write job self.config
        access_s3.write_job_config(Driver.JOB_ID, job_bucket, num_mappers, reducer_lambda_name,
                                   self.config["reducer"]["handler"], num_reducers)

        # Prepare Lambda functions
        data_encoding.zip_lambda(self.config["mapper"]["name"], self.config["mapper"]["zip"])
        data_encoding.zip_lambda(self.config["reducer"]["name"], self.config["reducer"]["zip"])
        data_encoding.zip_lambda(self.config["reducerCoordinator"]["name"], self.config["reducerCoordinator"]["zip"])
        data_encoding.zip_lambda(Driver.EXECUTOR_PATH, Driver.EXECUTOR_ZIP_PATH)

        # access_s3.write_to_s3(self.config["jobBucket"], Driver.CODE_MAPPER_PREFIX, self.config["mapper"]["zip"], {})
        # access_s3.write_to_s3(self.config["jobBucket"], Driver.CODE_REDUCER_PREFIX, self.config["reducer"]["zip"], {})

        print("The mapper zip path is", self.config["mapper"]["zip"])
        print("The code mapper prefix is", Driver.CODE_MAPPER_PREFIX)
        print("The reducer zip path is", self.config["reducer"]["zip"])
        print("The code reducer prefix zip path is", Driver.CODE_REDUCER_PREFIX)
        print("The job bucket is", self.config["jobBucket"])
        # self.s3_client.upload_file(self.config["mapper"]["zip"], self.config["jobBucket"], Driver.CODE_MAPPER_PREFIX)
        # self.s3_client.upload_file(self.config["reducer"]["zip"], self.config["jobBucket"], Driver.CODE_REDUCER_PREFIX)
        self.s3.Bucket(self.config["jobBucket"]).upload_file(self.config["mapper"]["zip"],
                                                             Driver.CODE_MAPPER_PREFIX + self.config["mapper"]["zip"])
        self.s3.Bucket(self.config["jobBucket"]).upload_file(self.config["reducer"]["zip"],
                                                             Driver.CODE_REDUCER_PREFIX + self.config["reducer"]["zip"])

        # Mapper
        # xray_recorder.begin_subsegment('Create mapper Lambda function')
        # l_mapper = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
        #                                         self.config["mapper"]["zip"], Driver.JOB_ID,
        #                                         mapper_lambda_name, self.config["mapper"]["handler"])
        # l_mapper.update_code_or_create_on_noexist()
        # xray_recorder.end_subsegment()  # Create mapper Lambda function
        #
        # # Reducer
        # xray_recorder.begin_subsegment('Create reducer Lambda function')
        # l_reducer = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
        #                                          self.config["reducer"]["zip"], Driver.JOB_ID,
        #                                          reducer_lambda_name, self.config["reducer"]["handler"])
        # l_reducer.update_code_or_create_on_noexist()
        # xray_recorder.end_subsegment()  # Create reducer Lambda function

        l = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                         Driver.EXECUTOR_ZIP_PATH, Driver.JOB_ID,
                                         executor_lambda_name, Driver.EXECUTOR_HANDLER_PATH)
        l.update_code_or_create_on_noexist()

        # Coordinator
        l_rc = lambda_manager.LambdaManager(self.lambda_client, self.s3_client, region,
                                            self.config["reducerCoordinator"]["zip"], Driver.JOB_ID,
                                            rc_lambda_name, self.config["reducerCoordinator"]["handler"])
        l_rc.update_code_or_create_on_noexist()

        # Add permission to the coordinator
        l_rc.add_lambda_permission(random.randint(1, 1000), job_bucket)

        # create event source for coordinator
        l_rc.create_s3_eventsource_notification(job_bucket, Driver.TASK_MAPPER_PREFIX + "bin" + str(num_reducers) + "/")
        return l, l_rc

    # Write Jobdata to S3
    def _write_job_data(self, all_keys, n_mappers):
        j_key = Driver.JOB_ID + "/jobdata"
        data = json.dumps({
            "mapCount": n_mappers,
            "totalS3Files": len(all_keys),
            "startTime": time.time()
        })

        # Write job data to S3
        access_s3.write_to_s3(self.config["jobBucket"], j_key, data, {})

    def invoke_lambda(self, mapper_outputs, batches, m_id):
        bucket = self.config["bucket"]
        '''
        aws_lambda invoke function
        '''
        job_bucket = self.config["jobBucket"]
        executor_lambda_name = Driver.L_PREFIX + "-executor-" + Driver.JOB_ID

        # batch = [k['Key'] for k in batches[m_id-1]]
        batch = [k.key for k in batches[m_id - 1]]
        # print "invoking", m_id, len(batch)
        resp = self.lambda_client.invoke(
            FunctionName=executor_lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "taskCode": Driver.CODE_MAPPER_PREFIX,
                "taskBucket": bucket,
                "taskInfo": {
                    "bucket": bucket,
                    "keys": batch,
                    "jobBucket": job_bucket,
                    "jobId": Driver.JOB_ID,
                    "mapperId": m_id
                }
            })
        )
        out = eval(resp['Payload'].read())
        mapper_outputs.append(out)
        print("Mapper Output:", out)

    def _invoke_mappers(self, n_mappers, batches):
        mapper_outputs = []

        concurrent_lambdas = self.config["concurrentLambdas"]

        # Exec Parallel
        print("# of Mappers ", n_mappers)
        pool = ThreadPool(n_mappers)
        Ids = [i + 1 for i in range(n_mappers)]
        invoke_lambda_partial = partial(self.invoke_lambda, mapper_outputs, batches)

        # Burst request handling
        mappers_executed = 0
        while mappers_executed < n_mappers:
            nm = min(concurrent_lambdas, n_mappers)
            results = pool.map(invoke_lambda_partial, Ids[mappers_executed: mappers_executed + nm])
            mappers_executed += nm

        pool.close()
        pool.join()

        print("all the mappers finished")
        return mapper_outputs

    def _calculate_cost(self, mapper_outputs):

        total_lambda_secs = 0
        total_s3_get_ops = 0
        total_s3_put_ops = 0
        s3_storage_hours = 0
        total_lines = 0

        for output in mapper_outputs:
            total_s3_get_ops += int(output[0])
            total_lines += int(output[1])
            total_lambda_secs += float(output[2])

        # Note: Wait for the job to complete so that we can compute total cost ; create a poll every 10 secs
        # Get all reducer keys

        # Total execution time for reducers
        reducer_lambda_time = 0
        job_bucket = self.config["jobBucket"]
        lambda_memory = self.config["lambdaMemory"]

        while True:
            response = self.s3_client.list_objects(Bucket=job_bucket, Prefix=Driver.TASK_REDUCER_PREFIX)
            if "Contents" in response:
                job_keys = response["Contents"]
                print("Checking whether the job is completed ...")
                # check job done
                if len(job_keys) == self.config["num_reducers"]:
                    print("Job Complete")
                    keys = [jk["Key"] for jk in job_keys]
                    total_s3_size = sum([jk["Size"] for jk in job_keys])
                    for key in keys:
                        reducer_lambda_time += float(self.s3.Object(job_bucket, key).metadata['processingtime'])
                    break

            time.sleep(5)

        # S3 Storage cost - Account for mappers only; This cost is neglibile anyways since S3
        # costs 3 cents/GB/month
        s3_storage_hour_cost = 1 * 0.0000521574022522109 * (
                    total_s3_size / 1024.0 / 1024.0 / 1024.0)  # cost per GB/hr
        s3_put_cost = len(job_keys) * 0.005 / 1000

        # S3 GET # $0.004/10000
        total_s3_get_ops += len(job_keys)
        s3_get_cost = total_s3_get_ops * 0.004 / 10000

        # Total Lambda costs
        total_lambda_secs += reducer_lambda_time
        lambda_cost = total_lambda_secs * 0.00001667 * lambda_memory / 1024.0
        s3_cost = (s3_get_cost + s3_put_cost + s3_storage_hour_cost)

        # Print costs
        print("Reducer L", reducer_lambda_time * 0.00001667 * lambda_memory / 1024.0)
        print("Lambda Cost", lambda_cost)
        print("S3 Storage Cost", s3_storage_hour_cost)
        print("S3 Request Cost", s3_get_cost + s3_put_cost)
        print("S3 Cost", s3_cost)
        print("Total Cost: ", lambda_cost + s3_cost)
        print("Total Lines:", total_lines)


    def run(self):

        # 1. Get all keys to be processed
        all_keys, n_mappers, batches = self._get_all_keys()

        # 2. Create the aws_lambda functions
        l, l_rc = self._create_lambda(n_mappers)
        self._write_job_data(all_keys, n_mappers)

        ### Execute ###
        # 3. Invoke Mappers and wait until they finish the execution
        mapper_outputs = self._invoke_mappers(n_mappers, batches)

        # 4. Delete Mapper function
        # l_mapper.delete_function()

        # 5. Calculate costs - Approx (since we are using exec time reported by our func and not billed ms)
        self._calculate_cost(mapper_outputs)

        # 6. Delete Reducer and its coordinator function
        # l_reducer.delete_function()
        l.delete_function()
        l_rc.delete_function()
