import time


# def Fibonacci(n):
#     if n < 0:
#         print("Incorrect input")
#     # First Fibonacci number is 0
#     elif n==1:
#         return 0
#     # Second Fibonacci number is 1
#     elif n==2:
#         return 1
#     else:
#         return (Fibonacci(n-1) + Fibonacci(n-2)) % 10000000
#
#
# def wrapper(x):
#     start = time.time()
#     res = Fibonacci(x)
#     end = time.time()
#     print("Time of computation:", end - start)
#     return res
#
#
# fib = wrapper(40)
# print("The result is:", fib)

start = time.time()
i = 0
while i < 10000:
    i += 1

end = time.time()
print("Time of computation:", end - start)