#!/home/ubuntu/ammunation/env/bin/python

import pandas as pd
from db import DatabaseLoader
import json
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.getenv("append_path"))

logging.basicConfig(
    filename="cabelas.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()


csv_file_list = [
    # "centerfire-rifles.csv",
    # "rimfire-pistols.csv",
    # "rimfire-rifle.csv",
    # "shotgun.csv",
    "centerfire-pistol.csv",
]
csv_file = csv_file_list[0]
filtered_data = []
database = DatabaseLoader()

for csv_file in csv_file_list:
    df = pd.read_csv(csv_file)
    df = df.where(pd.notna(df), None)

    for index, data in df.iterrows():
        flag = database.check_db_for_product(
            data["product_url"], source_code_name="cabelas"
        )
        if not flag:
            data_dict = {
                "title": data["title"],
                "selling_price": data["selling_price"],
                "marked_price": data["marked_price"],
                "discount_price": data["discount_price"],
                "sku": data["sku"],
                "model": None,
                "brand": data["brand"],
                "upc": None,
                "caliber": None,
                "description": data["description"],
                "availability": data["availability"],
                "features": data["features"],
                # "product_info": data["product_info"],
                "images": data["images"],
                "product_url": data["product_url"],
                "category": data["sub_category"],
            }
            filtered_data.append(data_dict)

# Convert the list of dictionaries to JSON
new_df = pd.DataFrame(filtered_data)
final_data = new_df.to_dict(orient="records")
database.insert_gun_products(final_data, source_code_name="cabelas")
