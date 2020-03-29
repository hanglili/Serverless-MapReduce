import os

from serverless_mr.main import set_up, tear_down
from unittest import TestCase


class Test(TestCase):
    @classmethod
    def setUp(self):
        print('\r\nSetting up and executing the job')
        set_up()
        os.environ["LOCALSTACK_HOSTNAME"] = '127.0.0.1'

    @classmethod
    def tearDown(self):
        print('\r\nTearing down the job')
        tear_down()
        del os.environ["LOCALSTACK_HOSTNAME"]

    def test_map_method(self):
        from user_job.map import map_function
        input_pair = (1, '127.0.0.1, null, null, 10.2')
        outputs = []
        map_function.__wrapped__(outputs, input_pair)
        print(outputs)
        assert len(outputs) == 1, "test failed"
        assert outputs[0] == ('127.0.0.1', 10.2), "test failed"

    def test_reduce_method(self):
        from user_job.reduce import reduce_function
        input_pair = ('127.0.0.1', [10.0, 20.0, 30.0, 48.0])
        outputs = []
        reduce_function.__wrapped__(outputs, input_pair)
        print(outputs)
        assert len(outputs) == 1, "test failed"
        assert outputs[0] == ('127.0.0.1', [108.0]), "test failed"
