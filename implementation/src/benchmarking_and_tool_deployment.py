import pandas as pd, numpy as np, time, psutil, os

dataframe = 'chunk_0000.parquet'
data = pd.read_parquet(dataframe)

def dedup_and_save(filepath):
    data = pd.read_parquet(filepath)
    #duplicates = file.duplicated()
    #duplicates.sum()
    dedup_data = data.drop_duplicates()
    dedup_data.to_csv(filepath, index = False)
    return dedup_data

def dedup(filepath):
    data = pd.read_parquet(filepath)
    return data.drop_duplicates()

def dedup_to_new_file(input_filepath, output_filepath):
    data = pd.read_parquet(input_filepath)
    dedup_data = data.drop_duplicates()
    dedup_data.to_parquet(output_filepath, index = False)
    return dedup_data

def memory(filepath):
    data = pd.read_parquet(filepath)
    byte = int(data.memory_usage(index = True).sum())
    print(f"File {filepath} has {byte} bytes.")
    return byte

def calculate_time(filepath):
    pass

def ram(file):
    pass

def cpu(filepath):
    return psutil.cpu_percent(interval = 1)

def measure_performance(function, *args, **kwargs):
    #Measures time, CPU percent and memory peak for a function.
    process = psutil.Process(os.getpid())
    start_time = time.time()
    start_memory = process.memory_info().rss
    result = function(*args, **kwargs)
    end_time = time.time()
    end_memory = process.memory_info().rss
    cpu = process.cpu_percent(interval=0.1)
    metrics = {
        "time_sec": end_time - start_time,
        "memory_mb": (end_memory - start_memory) / 1024**2,
        "cpu_percent": cpu
    }
    return result, metrics
