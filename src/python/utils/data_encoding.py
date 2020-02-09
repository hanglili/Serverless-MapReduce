import glob
import subprocess
from aws_xray_sdk.core import xray_recorder

JOB_INFO = "configuration/job-info.json"
MAP_HANDLER = "job/map_handler.py"
REDUCE_HANDLER = "job/reduce_handler.py"


@xray_recorder.capture('zip_lambda')
def zip_lambda(fname, zipname):
    # faster to zip with shell exec
    subprocess.call(['zip', zipname] + glob.glob(fname) + glob.glob(JOB_INFO) +
                    glob.glob("lambda_utils.py"))
    subprocess.call(['zip', zipname] + glob.glob(fname) + glob.glob(JOB_INFO) +
                    glob.glob("utils/lambda_utils.py") + glob.glob(MAP_HANDLER)
                    + glob.glob(REDUCE_HANDLER))
