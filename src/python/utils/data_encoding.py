import glob
import subprocess
from pathlib import Path

JOB_INFO = "configuration/job-info.json"
MAP_HANDLER = "job/map_handler.py"
REDUCE_HANDLER = "job/reduce_handler.py"
PARTITION = "job/partition.py"
COMBINE = "job/combine.py"
LAMBDA_UTILS = "utils/lambda_utils.py"
UTILS_INIT_FILE = "job/__init__.py"
JOB_INIT_FILE = "utils/__init__.py"
BOTO_LIBRARIES_DIR = "boto3/"
BOTO_LIBRARIES_DIR_2 = "boto3/*"
BOTO_LIBRARIES_DIR_3 = "boto3/*/*"
BOTO_LIBRARIES_DIR_4 = "boto3/*/*/*"
BOTO_LIBRARIES_DIR_5 = "boto3/*/*/*/*"
BOTOCORE_LIBRARIES_DIR_1 = "botocore/"
BOTOCORE_LIBRARIES_DIR_2 = "botocore/*"
BOTOCORE_LIBRARIES_DIR_3 = "botocore/*/*"
BOTOCORE_LIBRARIES_DIR_4 = "botocore/*/*/*"
BOTOCORE_LIBRARIES_DIR_5 = "botocore/*/*/*/*"
URLLIB3_LIBRARIES_DIR_1 = "urllib3/"
DATEUTIL_LIBRARIES_DIR_1 = "dateutil/"
JMESPATH_LIBRARIES_DIR_1 = "jmespath/"
DOCUTILS_LIBRARIES_DIR_1 = "docutils/"
FUTURE_LIBRARIES_DIR_1 = "future/"
S3TRANSFER_LIBRARIES_DIR_1 = "s3transfer/"
DISTLIB_LIBRARIES_DIR_1 = "distlib/"
PYTZ_LIBRARIES_DIR_1 = "pytz/"
JSONPICKLE_LIBRARIES_DIR_1 = "jsonpickle/"
CERTI_LIBRARIES_DIR_1 = "certi/"
LIBFUTURIZE_LIBRARIES_DIR_1 = "libfuturize/"
LIBPASTEURIZE_LIBRARIES_DIR_1 = "libpasteurize/"
WRAPT_LIBRARIES_DIR_1 = "wrapt/"
IMPORTLIB_LIBRARIES_DIR_1 = "importlib_metadata/"
PAST_LIBRARIES_DIR_1 = "past/"
SIX_LIBRARIES_FILE = "six.py"


def zip_lambda(filename, zip_name):
    # result = ' '.join(list(Path("boto3").rglob("*")))
    # result2 = ' '.join(list(Path("botocore").rglob("*")))
    result = glob.glob(BOTO_LIBRARIES_DIR + '**/*', recursive=True)
    result2 = glob.glob(BOTOCORE_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result3 = glob.glob(URLLIB3_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result4 = glob.glob(DATEUTIL_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result5 = glob.glob(JMESPATH_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result6 = glob.glob(DOCUTILS_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result7 = glob.glob(FUTURE_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result8 = glob.glob(S3TRANSFER_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result9 = glob.glob(DISTLIB_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result10 = glob.glob(PYTZ_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result11 = glob.glob(JSONPICKLE_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result12 = glob.glob(CERTI_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result13 = glob.glob(LIBFUTURIZE_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result14 = glob.glob(LIBPASTEURIZE_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result15 = glob.glob(WRAPT_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result16 = glob.glob(IMPORTLIB_LIBRARIES_DIR_1 + '**/*', recursive=True)
    result17 = glob.glob(PAST_LIBRARIES_DIR_1 + '**/*', recursive=True)
    # faster to zip with shell exec
    # subprocess.call(['zip', zip_name] + glob.glob(filename) + glob.glob(JOB_INFO)
    #                 + glob.glob(LAMBDA_UTILS) + glob.glob(MAP_HANDLER)
    #                 + glob.glob(REDUCE_HANDLER) + glob.glob(JOB_INIT_FILE) + glob.glob(UTILS_INIT_FILE)
    #                 + glob.glob(PARTITION) + glob.glob(COMBINE) + glob.glob(BOTO_LIBRARIES_DIR)
    #                 + glob.glob(BOTOCORE_LIBRARIES_DIR_2) + glob.glob(BOTO_LIBRARIES_DIR_3)
    #                 + glob.glob(BOTOCORE_LIBRARIES_DIR_1) + glob.glob(BOTOCORE_LIBRARIES_DIR_2)
    #                 + glob.glob(BOTOCORE_LIBRARIES_DIR_3) + glob.glob(BOTO_LIBRARIES_DIR_4)
    #                 + glob.glob(BOTOCORE_LIBRARIES_DIR_4) + glob.glob(BOTO_LIBRARIES_DIR_5) + glob.glob(BOTOCORE_LIBRARIES_DIR_5))

    subprocess.call(['zip', zip_name] + glob.glob(filename) + glob.glob(JOB_INFO)
                    + glob.glob(LAMBDA_UTILS) + glob.glob(MAP_HANDLER)
                    + glob.glob(REDUCE_HANDLER) + glob.glob(JOB_INIT_FILE) + glob.glob(UTILS_INIT_FILE)
                    + glob.glob(PARTITION) + glob.glob(COMBINE) + result + result2 + result3 + result4
                    + glob.glob(SIX_LIBRARIES_FILE) + result5 + result6 + result7 + result8 + result9
                    + result10 + result11 + result12 +result13 + result14 + result15 + result16 + result17)
