from user_job_5.map import extract_data_s3
from user_job_5.reduce import reduce_function
from unittest import TestCase


class Test(TestCase):
    def test_map_method(self):
        input_pair = (1, "127.0.0.1,djecvwmjzejguvrqaryffwwzdasozsslizligyozikvhdeodyvsnotlkldsuvtbcmzajlfdqoopeiqrpfhqhneqrpzdzrgshthe,1974-12-17,10.2,Xegqzir/8.7,PRT,PRT-PT,Portugalism,7\n")
        outputs = []
        extract_data_s3(outputs, input_pair)
        assert len(outputs) == 1, "test failed"
        assert outputs[0] == ('127.0.0.1', 10.2), "test failed"

    def test_reduce_method(self):
        input_pair = ('127.0.0.1', [10.0, 20.0, 30.0, 48.0])
        outputs = []
        reduce_function(outputs, input_pair)
        assert len(outputs) == 1, "test failed"
        assert outputs[0] == ('127.0.0.1', 108.0), "test failed"
