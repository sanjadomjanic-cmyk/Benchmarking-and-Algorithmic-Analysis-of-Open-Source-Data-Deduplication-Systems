import os, math, random, string, pyarrow as pa, pyarrow.parquet as pq
#Generiranje enostavnega parqueta. Približno 3 minute za 55MB

TARGET_GB =  #Končna velikost baze
CHUNK_ROWS = 10_000_000 #Vrstice na chunk (prilagodi glede na RAM)
OUTPUT_DIR = "random_parquet"
os.makedirs(OUTPUT_DIR, exist_ok = True)


def generate_rows(n, start_index):
    for i in range(n):
        row_id = start_index + i
        yield {
            "id": row_id,
            "column1": random.choice(string.ascii_letters),
            "column2": random.randint(1,10),
            "column3": random.choice(string.ascii_letters),
            "column4": random.randint(1,10),
        }

def make_table(n, start_index):
    batch = list(generate_rows(n, start_index))
    return pa.Table.from_pylist(batch)

print("Starting generation...\n")

current_size = 0
file_index = 0
row_index = 0

while current_size < TARGET_GB * 1024**3:
    table = make_table(CHUNK_ROWS, row_index)

    filename = f"{OUTPUT_DIR}/chunk_{file_index:04d}.parquet"
    pq.write_table(table, filename, compression = "snappy")

    file_size = os.path.getsize(filename)
    current_size += file_size
    row_index += CHUNK_ROWS
    file_index += 1

    print(f"Created {filename} ({file_size/1024**2:.2f} MB), "
          f"total = {current_size/1024**3:.2f} GB")

print("\nDONE!")
