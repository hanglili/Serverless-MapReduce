import inspect
import dill
import pickle

from pipeline.pipeline_test import map_function
print(inspect.getsource(map_function))
outputs = []
map_function(outputs, [1, '127.0.0.1, dasda, dasda, 1.0, dasdsa'])
print(outputs)
print()

print(dill.source.getsource(map_function))
# serialised_function = dill.dumps(map_function)

with open('pipeline/foo.pkl', 'wb') as f:
    pickle.dump(map_function, f)



