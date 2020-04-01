import subprocess

command = "aws --version"
command_list = command.split(' ')

print("Running shell command: \"{}\"".format(command))
result = subprocess.call(command_list, stdout=subprocess.PIPE)
print("Command output:\n---\n{}\n---".format(result.stdout.decode('UTF-8')))

# def write_reducer_state(n_reducers, n_s3, bucket, fname):
#     ts = time.time()
#     data = json.dumps({
#         "reducerCount": '%s' % n_reducers,
#         "totalS3Files": '%s' % n_s3,
#         "start_time": '%s' % ts
#     })
#     write_to_s3(bucket, fname, data, {})

# Count mapper files
# def get_num_finished_mappers(files):
#     bitmap_finished_mappers = [0 for x in range(num_mappers)]
#     for mf in files:
#         if "task/mapper" in mf["Key"]:
#             # print("The path is ", mf["Key"])
#             # print("Current bitmap is", bitmap_finished_mappers)
#             paths_components = mf["Key"].split("/")
#             mapper_id = paths_components[len(paths_components) - 1]
#             bitmap_finished_mappers[int(mapper_id) - 1] = 1
#
#     return sum(bitmap_finished_mappers)


# def get_reducer_batch_size(keys):
#     # TODO: Parameritize memory size
#     batch_size = lambda_utils.compute_batch_size(keys, 1536, 1000)
#     return max(batch_size, 2)  # At least 2 in a batch - Condition for termination
#
#
# def check_job_done(files):
#     # TODO: USE re
#     for f in files:
#         if "result" in f["Key"]:
#             return True
#     return False
#
#
# def get_reducer_state_info(files, job_id, job_bucket):
#     reducers = []
#     max_index = 0
#     reducer_step = False
#     r_index = 0
#
#     Check if step is complete
#
#     Check for the Reducer state
#     Determine the latest reducer step#
#     for f in files:
#         # parts = f['Key'].split('/');
#         if "reducerstate." in f['Key']:
#             idx = int(f['Key'].split('.')[1])
#             if idx > r_index:
#                 r_index = idx
#             reducer_step = True
#
#     Find with reducer state is complete
#     if reducer_step == False:
#         return mapper files
#     return [MAPPERS_DONE, get_mapper_files(files)]
#     else:
#         # Check if the current step is done
#         key = "%s/reducerstate.%s" % (job_id, r_index)
#         response = s3_client.get_object(Bucket=job_bucket, Key=key)
#         contents = json.loads(response['Body'].read())
#
#         # get reducer outputs
#         for f in files:
#             fname = f['Key']
#             parts = fname.split('/')
#             if len(parts) < 3:
#                 continue
#             rFname = 'reducer/' + str(r_index)
#             if rFname in fname:
#                 reducers.append(f)
#
#         if int(contents["reducerCount"]) == len(reducers):
#             return (r_index, reducers)
#         else:
#             return (r_index, [])

# Now write the reducer state
# fname = "%s/reducerstate.%s" % (job_id, step_id)
# write_reducer_state(n_reducers, n_s3, bucket, fname)

# stepInfo = get_reducer_state_info(files, job_id, bucket)

# print("stepInfo", stepInfo)

# step_number = stepInfo[0]
# reducer_keys = stepInfo[1]

# if len(reducer_keys) == 0:
#     print("Still waiting to finish Reducer step ", step_number)
#     return

# Compute this based on metadata of files
# r_batch_size = get_reducer_batch_size(reducer_keys)

# print("Starting the the reducer step", step_number)
# print("Batch Size", r_batch_size)

# Create Batch params for the Lambda function
# r_batch_params = lambda_utils.batch_creator(reducer_keys, r_batch_size)


# Build the lambda parameters
# num_reducers = len(r_batch_params)
# n_s3 = n_reducers * len(r_batch_params[0])
# step_id = step_number + 1

# @staticmethod
# def _setting():
#     # patch_all()
#     logging.basicConfig(level='WARNING')
#     # logging.getLogger('aws_xray_sdk').setLevel(logging.ERROR)
#     #
#     # # collect all tracing samples
#     # sampling_rules = {"version": 1, "default": {"fixed_target": 1, "rate": 1}}
#     # xray_recorder.configure(sampling_rules=sampling_rules)


# access_s3.py
# def write_to_s3(bucket, key, data, metadata):
#     s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='',
#                              region_name=StaticVariables.DEFAULT_REGION, endpoint_url='http://localhost:4572')
#     # s3_client = boto3.client('s3')
#     s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)


# def write_job_config(job_id, job_bucket, num_mappers, reducer_function, reduce_handler, reduce_count):
#     filename = StaticVariables.JOB_INFO_LAMBDA_PATH
#     with open(filename, 'w') as f:
#         data = json.dumps({
#             # "jobId": job_id,
#             # "jobBucket": job_bucket,
#             "mapCount": num_mappers,
#             # "reducerFunction": reducer_function,
#             # "reducerHandler": reduce_handler,
#             # "reduceCount": reduce_count
#         }, indent=4)
#         f.write(data)

# reducer_lambda_time += float(self.s3.Object(job_bucket, key).metadata['processingtime'])

# map_phase_state.py
# response = self.client.update_item(
#     TableName=table_name,
#     Key={
#         'id': {'N': str(MapPhaseState.DUMMY_ID)}
#     },
#     UpdateExpression="ADD #counter :increment",
#     ExpressionAttributeNames={'#counter': 'num_completed_mappers'},
#     ExpressionAttributeValues={':increment': {'N': '1'}},
#     ReturnValues="UPDATED_NEW"
# )
