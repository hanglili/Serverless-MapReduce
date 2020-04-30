import inspect
import dill
import pickle

from pipeline.pipeline_test import extract_data_s3

def hello():
    return "Hang is a genius"


print(inspect.getsource(hello))
outputs = []
# extract_data_s3(outputs, [1, '127.0.0.1, dasda, dasda, 1.0, dasdsa'])
# print(outputs)
print(hello())

print(dill.source.getsource(hello))
# serialised_function = dill.dumps(map_function)

with open('foo.pkl', 'wb') as f:
    pickle.dump(hello, f)

with open('foo.pkl', 'rb') as f:
    a = pickle.load(f)

print(str(a))



