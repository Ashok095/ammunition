import mysql.connector
from mysql.connector import errorcode
import json
import logging
from uuid import uuid4
from datetime import datetime

import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.getenv("append_path"))

logger = logging.getLogger()


class DatabaseLoader:
    def __init__(self) -> None:
        logger.info("setting up database variables")
        self.db_config = {
            "host": os.getenv("db_host"),
            "user": os.getenv("db_user"),
            "password": os.getenv("db_password"),
            "database": os.getenv("db_database"),
            "port": os.getenv("db_port"),
        }
        # print(self.db_config)
        self.insert_product_sql = """
            INSERT INTO guns_products (
            brand, title, product_url, description, category, features, availability,
            price, sale_price, sku, upc, guns_source_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        self.select_gun_source_sql = "SELECT id FROM guns_source WHERE code_name = %s"

        self.select_product_sql = "select product_url from guns_products where product_url = %s and guns_source_id = %s"

    def _connect_and_execute(self, query, params=None, columns=False):
        try:
            with mysql.connector.connect(**self.db_config, autocommit=True) as cnx:
                with cnx.cursor() as cursor:
                    logger.debug(f"Running {query}")
                    logger.debug(f"Got parameter - {params}")
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    if "insert" in query.lower():
                        # commit the changes
                        result = cursor.lastrowid
                        cnx.commit()
                    else:
                        result = cursor.fetchall()

                    # print(query)
                    # print(cursor.lastrowid)
                    # result = cursor.lastrowid if cursor.lastrowid else cursor.fetchall()
                    if columns:
                        # Fetch column names
                        columns = [desc[0] for desc in cursor.description]
                        return columns, result
                    return result
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Error: Access denied.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Error: Database does not exist.")
            else:
                logger.error(f"Error: {err}")
            return None

    def get_gun_source(self, source_code_name):
        logger.info(f"fetching gun source id using source code name {source_code_name}")
        query = "SELECT id FROM guns_source WHERE code_name = %s"
        result = self._connect_and_execute(query, (source_code_name,))
        if result:
            logger.info(f"got the gun source id - {result[0][0]}")
            return result[0][0]
        logger.info(f"source code name - {source_code_name} not found")
        return None

    def check_db_for_product(self, product_url, source_code_name):
        source_id = self.get_gun_source(source_code_name)
        if source_id:
            result = self._connect_and_execute(
                self.select_product_sql, (product_url, source_id)
            )
            if result:
                logger.info(f"Product with URL {product_url} already exists.")
                return True
            else:
                logger.info(f"Product with URL {product_url} does not exist.")
                return False
        else:
            logger.warning("Checking halted - code name not found")
            logger.warning(f"source code name {source_code_name} not found")

    def insert_gun_products(self, data, source_code_name):
        source_id = self.get_gun_source(source_code_name)
        if source_id:
            for product in data:
                brand = product["brand"]
                title = product["title"]
                product_url = product["product_url"]
                description = product["description"]
                # convert list to JSON string
                category = product["category"]
                # convert dictionary to JSON string
                features = json.dumps(product["features"])
                availability = product["availability"]
                price = product["price"]
                sale_price = product["sale_price"]
                sku = product["sku"]
                upc = product["upc"]

                product_data = (
                    brand,
                    title,
                    product_url,
                    description,
                    # images,
                    category,
                    features,
                    availability,
                    price,
                    sale_price,
                    sku,
                    upc,
                    source_id,
                )
                product_id = self._connect_and_execute(
                    self.insert_product_sql, product_data
                )
                logger.info(f"{title} - inserted successfully")
                for image in product["images"]:
                    logger.info(f"inserting image: {image} for product id {product_id}")
                    statement = (
                        """INSERT INTO product_media (product_id, link) VALUE (%s, %s)"""
                    )
                    image_id = self._connect_and_execute(statement, (product_id, image))
        else:
            logger.warning("Insertion halted")
            logger.warning(f"source code name {source_code_name} not found")

    def create_batch(self, source_code_name):
        statement = "INSERT INTO scrape_batch (batch_id, code_name, start_date) values (%s, %s, %s);"
        batch_id = uuid4().hex
        code_name = source_code_name
        start_date = datetime.now()
        # check_point =
        logger.info(f"creating new batch with id {batch_id}")
        self._connect_and_execute(
            statement,
            (
                batch_id,
                code_name,
                start_date,
            ),
        )
        logger.info(f"new batch with id {batch_id} created succesfully")
        return batch_id

    def fetch_lastest_batch(self, source_code_name):
        statement = f"SELECT batch_id, check_point FROM scrape_batch WHERE code_name='{source_code_name}' ORDER BY start_date DESC LIMIT 1;"
        result = self._connect_and_execute(statement)
        logger.debug(f"Got the latest batch result - {result}")
        if not result:
            batch_id = self.create_batch(source_code_name)
            check_point = None
        else:
            batch_id = result[0][0]
            check_point = result[0][1]
        logger.info(f"lastest batch id: {batch_id}")
        logger.info(f"lastest checkpoint: {check_point}")
        return batch_id, check_point

    def fetch_all_urls(self, batch_id, all=False):
        if all:
            statement = f"SELECT url FROM product_urls WHERE batch_id='{batch_id}'"
        else:
            statement = f"SELECT url FROM product_urls WHERE batch_id='{batch_id}' and is_fetched=False"
        result = self._connect_and_execute(statement)
        logger.debug(f"Got the url - {result}")
        urls = result
        logger.info(f"Returning list of urls")
        return urls

    def update_urls(self, batch_id, url):
        logger.info(f"updating url {url} of batch {batch_id}")
        statement = (
            "UPDATE product_urls SET is_fetched=True WHERE batch_id=%s AND url=%s"
        )
        params = (batch_id, url)
        self._connect_and_execute(statement, params)
        return True

    def create_product_url(self, urls, code_name):
        batch_id, check_point = self.fetch_lastest_batch(source_code_name=code_name)
        if isinstance(urls, str):
            urls = [urls]
        logger.info(f"Got {len(urls)} url to insert to Product url")
        for url in urls:
            url_already_exist = self.check_for_product_urls(url, batch_id)
            if url_already_exist:
                logger.info(
                    f"product url {url} already exist in the batch id {batch_id}"
                )
            else:
                try:
                    statement = "INSERT INTO product_urls (url, batch_id, code_name) VALUES (%s, %s, %s)"
                    params = (url, batch_id, code_name)
                    self._connect_and_execute(statement, params)
                    logger.info(f"Product URL '{url}' inserted successfully.")
                except Exception as e:
                    logger.error(f"Error inserting product URL '{url}': {e}")
        return batch_id

    def check_for_product_urls(self, url, batch_id):
        logger.info(f"Checking if the url {url} exist in the products url or not")
        statement = f"""SELECT url FROM product_urls where batch_id = '{batch_id}' and url ='{url}'"""
        result = self._connect_and_execute(statement)
        if result:
            logger.info(f"{url} exists in the product url with batch id {batch_id}")
            return True

        else:
            logger.info(
                f"{url} does not exist in the product url with batch id {batch_id}"
            )
            return False

    def update_check_point(self, url, code_name):
        logger.info("Updating url check point to the scrape_batch table")
        batch_id, check_point = self.fetch_lastest_batch(source_code_name=code_name)
        statement = (
            f"UPDATE scrape_batch SET check_point='{url}' WHERE batch_id='{batch_id}'"
        )
        result = self._connect_and_execute(statement)
        logger.info(f"check_point: {url} added successfully to the db")
        return batch_id

    def fetch_all_products(self, source_code_name, columns=False):
        logger.info(f"extracting all product of {source_code_name}")
        guns_source_id = self.get_gun_source(source_code_name=source_code_name)
        statement = (
            f"select * from guns_products where guns_source_id='{guns_source_id}'"
        )
        result = self._connect_and_execute(statement, columns=columns)
        return result
