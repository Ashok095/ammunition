#!/home/ubuntu/ammunation/env/bin/python
# 208.53.20.164

import pandas as pd
from db import DatabaseLoader
import json
import logging
import numpy as np
import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.getenv("append_path"))

logging.basicConfig(
    filename="midway_csv.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()


csv_file_list = [
    # "centerfire-rifles.csv",
    # "rimfire-pistols.csv",
    # "rimfire-rifle.csv",
    # "shotgun.csv",
    "midway.csv",
]
csv_file = csv_file_list[0]
filtered_data = []
database = DatabaseLoader()

for csv_file in csv_file_list:
    df = pd.read_csv(csv_file)
    df = df.where(pd.notna(df), None)
    # df = df.fillna(value=None)
    df = df.replace({pd.NA: None})

    for index, data in df.iterrows():
        flag = database.check_db_for_product(
            data["product_url"], source_code_name="midway"
        )
        if not flag:
            data_dict = {
                "title": data.get("title", None),
                "selling_price": data.get("selling_price", None),
                "marked_price": data.get("marked_price", None),
                "discount_price": data.get("discount_price", None),
                "sku": data.get("sku", None),
                "model": data.get("model", None),
                "brand": data.get("brand", None),
                "upc": data.get("upc", None),
                "caliber": data.get("caliber", None),
                "description": data.get("description", None),
                "availability": data.get("availability", None),
                "features": data.get("features", None),
                # "product_info": data["product_info"],
                "images": data.get("images", None),
                "product_url": data.get("product_url", None),
                "category": data.get("sub_category", None),
            }
            filtered_data.append(data_dict)

# Convert the list of dictionaries to JSON
new_df = pd.DataFrame(filtered_data)
final_data = new_df.replace(np.nan, None).to_dict(orient="records")
database.insert_gun_products(final_data, source_code_name="midway")
