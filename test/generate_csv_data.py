import csv
import os
import random
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

customer_ids = list(range(1, 501))
store_ids = list(range(121, 131))
product_data = {
    "premium basmati rice": ("Grocery", 180),
    "organic wheat flour": ("Grocery", 75),
    "tur dal premium": ("Grocery", 160),
    "chana dal": ("Grocery", 95),
    "sunflower cooking oil": ("Grocery", 140),
    "groundnut oil": ("Grocery", 165),
    "instant breakfast oats": ("Grocery", 220),
    "brown sugar": ("Grocery", 65),
    "rock salt": ("Grocery", 30),
    "multigrain atta": ("Grocery", 85),

    "herbal toothpaste": ("Personal Care", 110),
    "anti dandruff shampoo": ("Personal Care", 210),
    "moisturizing soap": ("Personal Care", 55),
    "face wash gel": ("Personal Care", 140),
    "body lotion": ("Personal Care", 240),

    "salted potato chips": ("Snacks", 25),
    "spicy corn puffs": ("Snacks", 30),
    "cream biscuits": ("Snacks", 35),
    "chocolate wafer rolls": ("Snacks", 45),

    "soya chunks": ("Grocery", 50)
}

sales_persons = {
    121: [1001, 1002, 1003],
    122: [1004, 1005, 1006],
    123: [1007, 1008, 1009],
    124: [1010, 1011, 1012],
    125: [1013, 1014, 1015],
    126: [1016, 1017, 1018],
    127: [1019, 1020, 1021],
    128: [1022, 1023, 1024],
    129: [1025, 1026, 1027],
    130: [1028, 1029, 1030]
}

end_date = datetime.today()
start_date = end_date - timedelta(days=180)
folder = os.getenv('PROJECT_FOLDER')

file_location = f"E:\\{folder}\\Project\\spark_data"
csv_file_path = os.path.join(file_location, "sales_data.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name", "category", "sales_date", "sales_person_id", "price", "quantity", "total_cost"])

    for _ in range(50000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        category,price = product_data[product_name]
        total_cost = price * quantity

        csvwriter.writerow([customer_id, store_id, product_name, category, sales_date.strftime("%Y-%m-%d"), sales_person_id, price, quantity, total_cost])

print("CSV file generated successfully.")