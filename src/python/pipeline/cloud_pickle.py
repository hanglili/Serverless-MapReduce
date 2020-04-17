import cloudpickle
import pickle

from pipeline.house import hello


# def foo(x):
#     return x*3
#
#
# def bar(z):
#     return foo(z)+1


# def house(size):
#     house = House()
#     house.set_size(size)

# with open('cloud_pickle.pkl', 'wb') as f:
#     pickle.dump(hello, f)

# del foo
# del bar
# del House
# del hello

with open('cloud_pickle.pkl', 'rb') as f:
    hello = pickle.load(f)

hello(1)
