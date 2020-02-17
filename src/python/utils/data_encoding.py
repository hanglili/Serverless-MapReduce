import glob
import subprocess

JOB_INFO = "configuration/job-info.json"
MAP_HANDLER = "job/map_handler.py"
REDUCE_HANDLER = "job/reduce_handler.py"
PARTITION = "job/partition.py"
COMBINE = "job/combine.py"
LAMBDA_UTILS = "utils/lambda_utils.py"
UTILS_INIT_FILE = "job/__init__.py"
JOB_INIT_FILE = "utils/__init__.py"


def zip_lambda(filename, zip_name):
    # faster to zip with shell exec
    subprocess.call(['zip', zip_name] + glob.glob(filename) + glob.glob(JOB_INFO)
                    + glob.glob(LAMBDA_UTILS) + glob.glob(MAP_HANDLER)
                    + glob.glob(REDUCE_HANDLER) + glob.glob(JOB_INIT_FILE) + glob.glob(UTILS_INIT_FILE)
                    + glob.glob(PARTITION) + glob.glob(COMBINE))
