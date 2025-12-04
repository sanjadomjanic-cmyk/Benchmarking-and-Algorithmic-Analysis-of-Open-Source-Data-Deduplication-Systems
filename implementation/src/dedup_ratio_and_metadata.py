import pyarrow.parquet as pq, pyarrow as pa, os

file_path = "chunk_0000.parquet"
parquet_file = pq.ParquetFile(file_path)
metadata = parquet_file.metadata

physical_size = os.path.getsize(file_path)
logical_size = 0
metadata_size = metadata.serialized_size

for i in range(metadata.num_row_groups):
    rg = metadata.row_group(i)
    for j in range(rg.num_columns):
        col = rg.column(j)
        logical_size += col.total_uncompressed_size

dedup_ratio = logical_size / physical_size

metadata_overhead = metadata_size / physical_size

print(f"Physical size (bytes): {physical_size}")
print(f"Logical size (bytes): {logical_size}")
print(f"Metadata size (bytes): {metadata_size}")

print(f"\nDedup ratio: {dedup_ratio:.2f}x")
print(f"Metadata overhead: {metadata_overhead:.2%}")
