import os, time, glob, duckdb, pandas as pd, pyarrow as pa, matplotlib
import pyarrow.dataset as ds, pyarrow.parquet as pq, matplotlib.pyplot as plt

DATA_PATH = "random.parquet" #"data/"
OUTPUT_CSV = "parquet_benchmark.csv"
OUTPUT_PLOT = "parquet_benchmark.png"

def list_parquet_files(path):
    if os.path.isfile(path):
        return [path]
    return glob.glob(os.path.join(path, "*.parquet"))

def get_file_sizes(files):
    return sum(os.path.getsize(f) for f in files)

def compute_metadata_and_logical_size(file):
    pf = pq.ParquetFile(file)
    metadata = pf.metadata

    logical_size = 0
    for rg in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg)
        for col in range(row_group.num_columns):
            logical_size += row_group.column(col).total_uncompressed_size

    metadata_size = metadata.serialized_size
    return logical_size, metadata_size

def measure_row_group_latency(file):
    pf = pq.ParquetFile(file)
    latencies = []
    for i in range(pf.num_row_groups):
        start = time.time()
        pf.read_row_group(i)
        latencies.append(time.time() - start)
    return sum(latencies) / len(latencies) if latencies else 0

def measure_full_scan_throughput(dataset, total_mb):
    start = time.time()
    dataset.to_table()
    elapsed = time.time() - start
    return total_mb / elapsed if elapsed > 0 else 0

def measure_column_scan_latency(dataset):
    start = time.time()
    dataset.to_table(columns=[dataset.schema.names[0]])
    return time.time() - start

def measure_duckdb_parallel_read(files):
    start = time.time()
    duckdb.query(f"SELECT COUNT(*) FROM '{files}'").df()
    return time.time() - start

files = list_parquet_files(DATA_PATH)
total_size_bytes = get_file_sizes(files)
total_size_mb = total_size_bytes / (1024 * 1024)

dataset = ds.dataset(files)

logical_total = 0
metadata_total = 0
for f in files:
    logical, metadata = compute_metadata_and_logical_size(f)
    logical_total += logical
    metadata_total += metadata

dedup_ratio = logical_total / total_size_bytes
metadata_overhead = metadata_total / total_size_bytes

throughput = measure_full_scan_throughput(dataset, total_size_mb)
column_latency = measure_column_scan_latency(dataset)
rg_latencies = [measure_row_group_latency(f) for f in files]
avg_row_group_latency = sum(rg_latencies) / len(rg_latencies)

parallel_time = measure_duckdb_parallel_read(os.path.join(DATA_PATH, "*.parquet"))

results = {
    "Metric": [
        "Dedup Ratio",
        "Metadata Overhead",
        "I/O Throughput (MB/s)",
        "Column Scan Latency (s)",
        "Row-Group Avg Latency (s)",
        "DuckDB Parallel Scan (s)"
    ],
    "Value": [
        dedup_ratio,
        metadata_overhead,
        throughput,
        column_latency,
        avg_row_group_latency,
        parallel_time
    ]
}

df = pd.DataFrame(results)
df.to_csv(OUTPUT_CSV, index=False)

plt.figure(figsize=(10, 6))
plt.bar(df["Metric"], df["Value"], color="steelblue")
plt.xticks(rotation=45, ha="right")
plt.title("Parquet Benchmark Results")
plt.tight_layout()
plt.savefig(OUTPUT_PLOT)
plt.close()

print("\n✔ Benchmark complete!")
print(f"CSV saved → {OUTPUT_CSV}")
print(f"Plot saved → {OUTPUT_PLOT}")
