import time

def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = round(end_time - start_time,3)
        print(f"Execution time of {func.__name__}: {execution_time} seconds")
        return result
    return wrapper

# Example usage
@timer
def slow_function():
    time.sleep(2)
    print("Function executed")

#slow_function()