import os, math, pyarrow as pa, pyarrow.parquet as pq
from faker import Faker
from PIL import Image, ImageDraw
#Generiranje parqueta z generiranjem slik. Približno 15 minut za 56,28MB

TARGET_GB = 1 #Končna velikost baze
CHUNK_ROWS = 200_000 #Vrstice na chunk (prilagodi glede na RAM)
OUTPUT_DIR = "big_parquet_with_images"
IMAGE_DIR = os.path.join(OUTPUT_DIR, "images")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)

fake = Faker()
Faker.seed(42)


def generate_image(path, text):
    img = Image.new("RGB", (512, 512), (fake.random_int(0,255),
                                        fake.random_int(0,255),
                                        fake.random_int(0,255)))
    draw = ImageDraw.Draw(img)
    draw.text((20, 20), text, fill=(255,255,255))
    img.save(path, format="JPEG", quality=70)


def generate_rows(n, start_index):
    for i in range(n):
        row_id = start_index + i

        #generate image path
        image_filename = f"img_{row_id}.jpg"
        image_path = os.path.join(IMAGE_DIR, image_filename)

        #generate a simple image
        generate_image(image_path, f"ID {row_id}")

        yield {
            "id": row_id,
            "name": fake.name(),
            "email": fake.email(),
            "job": fake.job(),
            "company": fake.company(),
            "address": fake.address(),
            "created_at": fake.iso8601(),
            "text": fake.text(max_nb_chars=400),
            "image_path": image_path,     # Store path in Parquet
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
