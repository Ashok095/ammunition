#!/home/ubuntu/ammunation/env/bin/python
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.getenv("append_path"))

# import re
import os

import logging
import datetime
from pathlib import Path
import requests
import re
import json


from db import DatabaseLoader


# Get today's date
todays_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
# Create a formatter with timestamp
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(lineno)s - %(message)s"
)
# Create a file handler with the formatter
log_path = Path(__file__).parent / "logs" / f"cabelas_{todays_date}.log"
file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
file_handler.setFormatter(formatter)
# Get the root logger
logger = logging.getLogger()
# Set the logging level
logger.setLevel(logging.INFO)
# Clear any existing handlers
logger.handlers = []
# Add the file handler to the logger
logger.addHandler(file_handler)

json_path = log_path = Path(__file__).parent / "cabelas.json"


class CabelasExtractor:
    def __init__(self) -> None:
        self.max_retires = 4
        self.token = None
        self.db = DatabaseLoader()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.cabelas.com/",
            "Content-Type": "application/json",
            "Content-Length": "695",
            "Origin": "https://www.cabelas.com",
            "DNT": "1",
            "Sec-GPC": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "TE": "trailers",
        }

    def _get_authorization(self):
        url = "https://www.cabelas.com/c/guns"
        response = requests.get(url, headers=self.headers)
        key = re.findall(r'token\":"(.*?)"', response.text)
        authorization = "".join(key)
        return authorization

    def get_data(self):
        for i in range(self.max_retires):
            self.token = self._get_authorization()
            url = "https://platform.cloud.coveo.com/rest/search/v2"
            params = {"organizationId": "bassproshopsproductionl92epymr"}
            self.headers["Authorization"] = f"Bearer {self.token}"

            with open(str(json_path), "r") as f:
                payload = json.load(f)
            resp = requests.post(
                url, headers=self.headers, params=params, data=json.dumps(payload)
            )
            if resp.status_code == 200:
                result = resp.json()
                data = self.extract_data(result)
                return data
            elif resp.status_code == 401:
                logger.info(f"error {resp.status_code}")
                logger.info(f"error {resp.content}")

                continue
            else:
                logger.info(f"error {resp.status_code}")
                logger.debug(f"error {resp.content}")
                continue
        raise RuntimeError("error occured while extracting Cabelas products")

    def extract_data(self, result):
        all_data = result["results"]
        all_output = []
        for data in all_data:
            url = "https://www.cabelas.com/shop/en/{}"
            product_data = data["raw"]
            # category_name = product_data.get("categoryname", None)
            category = product_data.get("type", "guns")
            product_count = product_data.get("availquantity", 0)
            availability = 1 if int(product_count) > 0 else 0
            features = {
                "model_number": product_data.get("model_number", None),
                "dimensionweight": product_data.get("dimensionweight", None),
                "dimensionwidth": product_data.get("dimensionwidth", None),
                "dimensionlength": product_data.get("dimensionlength", None),
                "dimensionheight": product_data.get("dimensionheight", None),
                "type": product_data.get("type", None),
                "gun_weight": product_data.get("gun_weight", None),
                "size": product_data.get("size", None),
                "department_name": product_data.get("department_name", None),
                "class_name": product_data.get("class_name", None),
                "action": product_data.get("action", None),
                "barrel_length": product_data.get("barrel_length", None),
                "eccn": product_data.get("eccn", None),
                "cartridge_or_gauge": product_data.get("cartridge_or_gauge", None),
                "round_capacity": product_data.get("round_capacity", None),
                "finish": product_data.get("finish", None),
                "stock_color": product_data.get("stock_color", None),
            }

            image_dict = {
                "fullimage": product_data.get("fullimage", None),
                "productcolorimagelist": product_data.get(
                    "productcolorimagelist", None
                ),
                "ec_thumbnails": product_data.get("ec_thumbnails", None),
                "topthumbnail": product_data.get("topthumbnail", None),
                "thumbnail": product_data.get("thumbnail", None),
                "product_color_image": product_data.get("product_color_image", None),
            }
            images = [value for key, value in image_dict.items() if value]
            marked_price_raw = product_data.get("listprice", None)
            marked_price = float(marked_price_raw) if marked_price_raw else None
            selling_price_raw = product_data.get("listprice", None)
            selling_price = float(selling_price_raw) if selling_price_raw else None
            output = {
                "title": data["title"],
                "brand": product_data.get("ec_brand", None),
                "product_url": url.format(product_data.get("producturlkeyword", None)),
                "description": product_data["ec_description"],
                "images": json.dumps(images),
                "category": category,
                "features": json.dumps(features),
                "availability": availability,
                "marked_price": marked_price,
                "selling_price": selling_price,
                "upc": product_data.get("upc", None),
                "sku": product_data.get("sku", None),
                "discount_price": product_data.get("maxsavings", None),
            }
            all_output.append(output)
        return all_output

    def extract(self):
        all_data = self.get_data()
        for data in all_data:
            url = data["product_url"]
            flag = self.db.check_db_for_product(url, "cabelas")
            if not flag:
                logger.info(f"url: {url} does not exist")
                logger.debug("Inserting data: ", data)
                logger.info(f"inserting {url}")
                self.db.insert_gun_products([data], "cabelas")
            else:
                logger.info(f"url: {url} does exist")
