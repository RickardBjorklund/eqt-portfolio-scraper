import json
import time
from functools import wraps

import pandas


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
def json_file_to_data_frame(file):
    return pandas.read_json(file, lines=True)


@time_function
def write_json_file(file, data):
    # Serializing json
    json_object = json.dumps(data, indent=4)

    # Writing to file
    with open(file, "w", encoding="utf8") as json_file:
        json_file.write(json_object)


@time_function
def save_df_to_file(df):
    import datetime

    current_time = datetime.datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d_%H-%M-%S")
    filepath = f"../results/result_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as file:
        df.to_json(path_or_buf=file, orient="records", indent=4, force_ascii=False)
