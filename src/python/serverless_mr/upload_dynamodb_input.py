import json
import glob

from serverless_mr.static.static_variables import StaticVariables
from serverless_mr.utils import input_handler
from serverless_mr.main import set_up, tear_down

set_up()

static_job_info_file = open(StaticVariables.STATIC_JOB_INFO_PATH, "r")
static_job_info = json.loads(static_job_info_file.read())
static_job_info_file.close()

cur_input_handler = input_handler.get_input_handler(static_job_info[StaticVariables.INPUT_SOURCE_TYPE_FN])

local_testing_input_path = static_job_info[StaticVariables.LOCAL_TESTING_INPUT_PATH]
local_file_paths = glob.glob(local_testing_input_path + "*")
cur_input_handler.set_up_local_input_data(local_file_paths)

tear_down()