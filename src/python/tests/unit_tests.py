from job.reduce import reduce_function
from job.map import map_function
from unittest import TestCase


# class Test(TestCase):
def test_map_method():
    input_pair = (1, '127.0.0.1, null, null, 10.2')
    outputs = []
    map_function.__wrapped__(outputs, input_pair)
    print(outputs)
    assert len(outputs) == 1, "test failed"
    assert outputs[0] == ('127.0.0.1', 10.2), "test failed"

def test_reduce_method():
    input_pair = ('127.0.0.1', [10.0, 20.0, 30.0, 48.0])
    outputs = []
    reduce_function.__wrapped__(outputs, input_pair)
    print(outputs)
    assert len(outputs) == 1, "test failed"
    assert outputs[0] == ('127.0.0.1', [108.0]), "test failed"
