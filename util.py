from functools import wraps


def defer(func):
    """ Function annotation @defer, executes cleanup functions after annotated functions are run
    Also executes if the wrapped function raises an exception
    Syntax is
    @defer
    def main(defer):
        file = open('test.txt')
        defer(lambda: file.close())
    lambda has to be used to avoid exection at defer time
    Based on https://towerbabbel.com/categories/webdev/"""
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        deferred_functions = []
        def defer(f): return deferred_functions.append(f)
        try:
            return func(*args, defer=defer, **kwargs)
        finally:
            for func in reversed(deferred_functions):
                func()
        return func_wrapper
