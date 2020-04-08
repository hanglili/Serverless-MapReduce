import json
from serverless_mr.static.static_variables import StaticVariables

def compute_batch_size(keys, lambda_memory, concurrent_lambdas):
    static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, "r")
    static_job_info = json.loads(static_job_info_file.read())
    static_job_info_file.close()
    input_source_type = static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN]
    max_mem_for_data = 0.6 * lambda_memory * 1000 * 1000
    size = 0.0
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size
    avg_object_size = size / len(keys)
    print("Dataset size: %s, nKeys: %s, avg object size: %s" % (size, len(keys), avg_object_size))
    if avg_object_size < max_mem_for_data and len(keys) < concurrent_lambdas:
        # Heuristics based on the input source type and assumption made on the convention of each data storage.
        b_size = 1 if input_source_type == "s3" else 5
    else:
        b_size = int(round(max_mem_for_data / avg_object_size))
    return b_size


def batch_creator(all_keys, batch_size):
    # TODO: Create optimal batch sizes based on key size & number of keys. Use queueing theory?

    batches = []
    batch = []
    for key in all_keys:
        batch.append(key)
        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []

    if len(batch) > 0:
        batches.append(batch)
    return batches
