def unwrap(fn):
    return fn.__wrapped__


def squared(method):
    print("1")
    def wrapper(x, y):
        print("Dsds")
        return method(x*x, y*y)
    wrapper.__wrapped__ = method
    print("2")
    return wrapper

@squared
def sum(x, y):
    return x+y


print(sum(2, 3))
print(sum(2, 3))

print(unwrap(sum)(2, 3))
