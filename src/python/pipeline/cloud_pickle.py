import cloudpickle
import pickle

# from pipeline.house import House


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
#     cloudpickle.dump(bar, f)

# del foo
# del bar
# del House

with open('cloud_pickle.pkl', 'r') as f:
    house = cloudpickle.loads(f)
