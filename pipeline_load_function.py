import dill
import pickle

outputs = []
with open('src/python/pipeline/foo.pkl', 'rb') as f:
    map_function_ = pickle.load(f)

map_function_(outputs, [1, '127.0.0.1, dasda, dasda, 1.0, dasdsa'])
print(outputs)