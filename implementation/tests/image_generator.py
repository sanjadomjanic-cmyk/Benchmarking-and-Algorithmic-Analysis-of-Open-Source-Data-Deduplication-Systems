import faker
from PIL import Image, ImageDraw

fake = faker.Faker()

def generate_image(path, text):
    img = Image.new("RGB", (512, 512), (fake.random_int(0,255),
                                        fake.random_int(0,255),
                                        fake.random_int(0,255)))
    draw = ImageDraw.Draw(img)
    draw.text((20, 20), text, fill=(255,255,255))
    img.save(path, format="JPEG", quality=70)
