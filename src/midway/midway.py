#!/home/ubuntu/ammunation/env/bin/python
import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.getenv('append_path'))

from algoliasearch.search_client import SearchClient
import requests
import pandas as pd
import time
import re
from db import DatabaseLoader


import logging
from datetime import datetime
from pathlib import Path
import json
from midway_utils import (
    convert_to_nested_dict,
    get_count_and_id,
    get_id_count_from_response,
    create_api_urls_from_data,
    get_user_agent,
    extract_details_from_response,
    get_family_and_object_id,
)


# Get today's date
todays_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
# Create a formatter with timestamp
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(lineno)s - %(message)s"
)
# Create a file handler with the formatter
log_path = Path(__file__).parent / "logs" / f"midway_{todays_date}.log"
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


class MidWayExtractor:
    def __init__(self):
        self.payload = {
            "api_key": "d3ac880a20ac99c18859cfac13de7a03",
            "url": None,
        }
        self.category_id = "23850"
        self.client = SearchClient.create(
            "UQIWQHWTGQ", "ba4f024807ab1f7f9d863f7d7ee61e7d"
        )
        self.index = self.client.init_index("p_product")
        self.algolia_payload = {
            "X-Algolia-UserToken": "Algolia for JavaScript (4.14.2); Browser (lite)",
            "attributesToRetrieve": ["familyId", "objectId", "sku", "upc"],
            "page": 0,
            "facets": ["categoryLevelsNew", "Type"],
            "responseFields": ["facets", "hits"],
        }

        self.base_api_url = "https://www.midwayusa.com/api/product/data?id={}&pid={}"
        self.base_product_url = "https://www.midwayusa.com/product/{}?pid={}"
        self.session = requests.Session()
        self.database = DatabaseLoader()
        self.max_retires = 10

    def get_algoliasearch_result(self, facet_filter: str, hit_per_page: int) -> dict:
        self.algolia_payload["hitsPerPage"] = hit_per_page
        self.algolia_payload["facetFilters"] = facet_filter
        response = self.index.search("guns", self.algolia_payload)
        logger.debug(
            f"get_algoliasearch_result - Got result for {facet_filter} - result \n {response}"
        )
        logger.info(f"get_algoliasearch_result - Got the response - Returning")
        return response

    def perform_initial_algoliasearch(self, category_id: str) -> dict:
        facet_filter = [["visible:true"], [f"categoryIds:{category_id}"]]
        response = self.get_algoliasearch_result(facet_filter, 1)
        return response

    def get_facet_filter_list(self, object_id_count_list: list) -> list:
        facet_filters_list = []
        for object_data in object_id_count_list:
            if object_data["count"] > 1000:
                category_id = object_data.get("id")
                facet_filters = [["visible:true"], [f"categoryIds:{category_id}"]]
                response = self.get_algoliasearch_result(facet_filters, 1)
                facet_types = response["facets"]["Type"]
                category_types = []
                for name, count in facet_types.items():
                    if facet_types[name] > 0:
                        category_types.append(name)
                for category_type in category_types:
                    facet_filters = [
                        ["visible:true"],
                        [f"categoryIds:{category_id}"],
                        [f"Type:{category_type}"],
                    ]
                    facet_filters_list.append(facet_filters)
            else:
                facet_filters = [["visible:true"], [f"categoryIds:{object_data['id']}"]]
                facet_filters_list.append(facet_filters)
        return facet_filters_list

    def get_all_objects_and_family_ids(self, facet_filters: list, all=False) -> list:
        all_data = []
        for facet_filter in facet_filters:
            response = self.get_algoliasearch_result(facet_filter, 1000)
            data = response["hits"]
            if all:
                all_data.extend(data)
            else:
                try:
                    for record in data:
                        familyid = record["familyId"]
                        objectid = record["objectID"]
                        all_data.append(
                            {
                                "familyId": record["familyId"],
                                "objectID": record["objectID"],
                            }
                        )

                except Exception as e:
                    print(record)
        return all_data

    def process_algoliasearch(self, category_id=None):
        category_id = category_id if category_id else self.category_id
        initial_response = self.perform_initial_algoliasearch(category_id)
        object_id_count_list = get_id_count_from_response(initial_response)
        facet_filters_list = self.get_facet_filter_list(object_id_count_list)
        data = self.get_all_objects_and_family_ids(facet_filters_list)
        url_list = create_api_urls_from_data(data)
        self.database.create_product_url(url_list, code_name="midway")
        return data

    def get_api_resonse_from_request(self, url):
        for i in range(self.max_retires):
            try:
                headers_data = {
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-US,en;q=0.9,ne;q=0.8",
                    "Cache-Control": "no-store, no-cache, must-revalidate, post-check=0, pre-check=0",
                    "Content-Type": "application/json",
                    "Dnt": "1",
                    "Expires": "0",
                    "Pragma": "no-cache",
                    "Referer": "https://www.midwayusa.com",
                    "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                    "Sec-Ch-Ua-Mobile": "?1",
                    "Sec-Ch-Ua-Platform": '"Android"',
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
                }

                user_agent, platform = get_user_agent()
                response = self.session.get(url, headers=headers_data)
                if "json" in response.headers["Content-Type"]:
                    result = response.json()
                    return result
                else:
                    logger.error(
                        f"{i} - Got {response.headers['Content-Type']} with status code {response.status_code}"
                    )
                    continue
            except Exception as e:
                logger.error(f"Got error {e}")
                logger.info("exception - Sleeping for 1 minute")
                time.sleep(60)
                logger.info("exception - waking up after error sleeping")

        logger.error(f"max retires exceed throwing error now")
        raise RuntimeError("Max retires exceed. now throwing error")

    def get_api_response_from_scrapper(self, api_url):
        base_url = "https://api.scraperapi.com/"
        # https://www.scraperapi.com
        self.payload["url"] = api_url
        try:
            result = requests.get(base_url, data=self.payload)
        except Exception as e:
            logger.error(f"get_api_response_from_scrapper -Got error {e}")
            print(f"get_api_response_from_scrapper -Got error {e}")
            logger.info(
                "get_api_response_from_scrapper -exception - Sleeping for 1 minute"
            )
            time.sleep(60)
            logger.info(
                "get_api_response_from_scrapper -exception - waking up after error sleeping"
            )
            logger.info(
                "get_api_response_from_scrapper -exception - Requesting the same url"
            )
            result = requests.get(base_url, data=self.payload)
            logger.info(
                f"get_api_response_from_scrapper -exception - now got the {result.status_code} after error"
            )
        else:
            logger.debug(
                f"get_api_response_from_scrapper -try-else - Got result with status code {result.status_code}"
            )
            logger.debug(
                f"get_api_response_from_scrapper -try-else - Got result with status code {result.status_code} result:- {result.content}"
            )
        return result

    def scrape_product_by_url(self, urls: list) -> list:
        if isinstance(urls, str):
            urls = [urls]
        all_data = []
        for url in urls:
            # flag = self.database.check_db_for_product(url)
            # if not flag:
            logger.info("Scraping url...")
            data = self.get_api_resonse_from_request(url)

            family_id = data["productFamily"].get("familyNumber", None)
            if not family_id: 
                family_id = get_family_and_object_id(url)
            if not family_id:
                continue
            all_sales_items = []
            for filter_group in data["filterGroups"]:
                for filter_option in filter_group["filterOptions"]:
                    sales_item_list = filter_option["saleItemIds"]
                    all_sales_items.extend(sales_item_list)
            product_apis = [
                self.base_api_url.format(family_id, object_id)
                for object_id in list(set(all_sales_items))
            ]
            product_urls = [
                self.base_product_url.format(family_id, object_id)
                for object_id in list(set(all_sales_items))
            ]
            for r, product_api_url in enumerate(product_apis):
                product_url = product_urls[r]
                flag = self.database.check_db_for_product(product_url, "midway")
                if not flag:
                    response = self.get_api_resonse_from_request(product_api_url)

                    product_data = extract_details_from_response(response)
                    all_data.extend(product_data)
                    self.database.insert_gun_products(
                        product_data, source_code_name="midway"
                    )
        return all_data

    def scrape_product_by_data(self, data: list) -> list:
        all_data = []
        for records in data:
            api_url = self.base_api_url.format(records["familyId"], records["objectID"])
            product_data = self.scrape_product_by_url(api_url)
            all_data.extend(product_data)
        return all_data

    def get_url_from_db(self):
        logger.info("Fetching latest batch id from db")
        batch_id, check_point = self.database.fetch_lastest_batch(
            source_code_name="midway"
        )
        logger.info(f"Found batch - {batch_id}")

        logger.info(f"Fetching all urls where is_fetched is false:")
        urls = self.database.fetch_all_urls(batch_id=batch_id)
        logger.debug(f"Got url from product urls table: {urls}")
        logger.info(f"Got url from product urls table: {len(urls)}")
        logger.debug("Returning batchid and urls")
        return batch_id, urls

    def scrape_product_from_db(self):
        logger.info("Fetching latest batch id from db")
        batch_id, urls = self.get_url_from_db()
        for url in urls:
            # format is [(url,), (url,)]
            url = url[0]
            flag = self.database.check_db_for_product(url, source_code_name="midway")
            if not flag:
                self.scrape_product_by_url(url)
            else:
                logger.info("Product details already extracted. skipping for now")
            self.database.update_urls(batch_id, url)
        return

    def scrape_all_guns(self):
        data = self.process_algoliasearch()
        product_list = self.scrape_product_by_data(data)
        return product_list
