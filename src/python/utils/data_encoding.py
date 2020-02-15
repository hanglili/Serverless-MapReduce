import glob
import subprocess
# from aws_xray_sdk.core import xray_recorder

JOB_INFO = "configuration/job-info.json"
MAP_HANDLER = "job/map_handler.py"
REDUCE_HANDLER = "job/reduce_handler.py"
PARTITION = "job/partition.py"
COMBINE = "job/combine.py"
UTILS_INIT_FILE = "job/__init__.py"
JOB_INIT_FILE = "utils/__init__.py"


# @xray_recorder.capture('zip_lambda')
def zip_lambda(fname, zipname):
    # faster to zip with shell exec
    subprocess.call(['zip', zipname] + glob.glob(fname) + glob.glob(JOB_INFO)
                    + glob.glob("utils/lambda_utils.py") + glob.glob(MAP_HANDLER)
                    + glob.glob(REDUCE_HANDLER) + glob.glob(JOB_INIT_FILE) + glob.glob(UTILS_INIT_FILE)
                    + glob.glob(PARTITION) + glob.glob(COMBINE))


def zip_driver_lambda(fname, zipname):
    # faster to zip with shell exec
    subprocess.call(['zip', zipname] + glob.glob("*") + glob.glob("*/*"))
