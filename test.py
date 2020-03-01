def unwrap(fn):
    return fn.__wrapped__

def squared(method):
    def wrapper(x, y):
        return method(x*x, y*y)
    wrapper.__wrapped__ = method
    return wrapper

@squared
def sum(x, y):
    return x+y

print(unwrap(sum)(2, 3))
