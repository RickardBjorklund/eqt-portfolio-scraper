import datetime
import json
import time
from functools import wraps


def time_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        elapsed = end - start
        print(f"Function: {func.__name__} took {elapsed:.6f} seconds to execute")
        return result
    return wrapper


@time_function
def read_json_file(file):
    with open(file, "r", encoding="utf8") as json_file:
        return json.load(json_file)


@time_function
def save_df_to_file(df):
    current_time = datetime.datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d_%H-%M-%S")
    filepath = f"results/result_{timestamp}.json"  # TODO: Use os.path.join ?

    with open(filepath, "w", encoding="utf-8") as file:
        df.to_json(path_or_buf=file, orient="records", indent=4, force_ascii=False)
