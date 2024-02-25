#!/home/ubuntu/ammunation/env/bin/python
import os
import sys
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.getenv("append_path"))

# import re
import os
import time

# import json
# import requests
import logging
import datetime
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup as bs
from contextlib import contextmanager


# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
import undetected_chromedriver as uc

from fake_useragent import UserAgent
from db import DatabaseLoader

from utils_psa import (
    get_product_data,
    get_products_urls,
    is_next_button_available,
    is_next_page_available,
    check404,
)

# Get today's date
todays_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
# Create a formatter with timestamp
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(lineno)s - %(message)s"
)
# Create a file handler with the formatter
log_path = Path(__file__).parent / "logs" / f"palmetto_state_armory_{todays_date}.log"
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


class ExtractPalmettoStateArmory:
    def __init__(self):
        self.driver = None
        self.database = DatabaseLoader()
        self.max_retries = 5
        # self.chrome_profile_path = os.getenv("google_profile_path")
        # self.driver_failed = False


    def _create_chrome_options(self):
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument("--enable-javascript")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        # chrome_options.add_argument("--user-data-dir=" + str(self.chrome_profile_path))

        chrome_options.headless = True
        chrome_options.use_subprocess = False
        return chrome_options

    def _get_user_agent(self, os="linux"):
        if os == "linux":
            platform = "Linux x86_64"
        else:
            platform = "Windows"
        ua = UserAgent(os=os, browsers=["chrome"])
        user_agent = ua.random
        return user_agent, platform

    @contextmanager
    def webdriver_session(self):
        self.driver = None
        try:
            logger.info(f"Starting WebDriver attempt")
            chrome_options = self._create_chrome_options()
            self.driver = uc.Chrome(options=chrome_options, port=9222, version_main=121)
            user_agent, platform = self._get_user_agent()
            self.driver.set_page_load_timeout(60)
            self.driver.execute_cdp_cmd(
                "Network.setUserAgentOverride",
                {"userAgent": user_agent, "platform": platform},
            )
            yield self.driver
        except Exception as e:
            logger.error(f"Error starting driver: {e}")
            raise RuntimeError("Failed to start driver")

        finally:
            if self.is_webdriver_alive():
                try:
                    # Attempt to quit regardless of errors
                    self.driver.quit()
                except Exception as e:
                    logger.error(f"Error during driver quit: {e}")
                # Ensure attribute is reset
            self.driver = None

    def soup_generator(self, url) -> bs:
        logger.info("")
        logger.info("")
        logger.info("=" * 50)
        logger.info(f"Getting soup element for URL: {url}")
        with self.webdriver_session() as driver:
            try:
                # Navigate to the URL and handle age verification
                driver.get(url)
                time.sleep(5)
                # Adjust timeout as needed
                driver.set_script_timeout(60)
                # Wait for the element with id="maincontent" to be present
                WebDriverWait(driver, 50).until(
                    EC.presence_of_element_located((By.ID, "maincontent"))
                )

                self.handle_age_verification()
                soup = bs(driver.page_source, "html.parser")
                logger.info("Returning soup element.")
                return soup
                # return driver

            except TimeoutException:
                logger.error(
                    "Timeout: The element with id='maincontent' did not appear in 50 seconds"
                )
                current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                png_name = f"timeout-{current_date}.png"
                driver.save_screenshot(png_name)
                logger.error("Timeout error. retrying again")
                return False

            except Exception as e:
                logger.error(f"Error: {e}")
                current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                png_name = f"error-{current_date}.png"
                driver.save_screenshot(png_name)
                logger.error("could not get the soup")
                return False
            finally:
                if self.is_webdriver_alive():
                    self.driver.quit()
                else:
                    self.driver = None

    def get_soup_element(self, url):
        for attempts in range(self.max_retries):
            soup = self.soup_generator(url)
            if soup:
                logger.info(f"Soup found in attempt: {attempts+1}")
                return soup
            else:
                if self.is_webdriver_alive():
                    try:
                        # Attempt to quit regardless of errors
                        self.driver.quit()
                    except Exception as e:
                        logger.error(f"Error during driver quit: {e}")
                # Ensure attribute is reset
                self.driver = None
                logger.warning("Soup not found, taking rest for 100 seconds")
                time.sleep(100)
                logger.info("waking up after 100 seconds")
                print("soup issue occured")
        logger.error(f"Soup not found after {self.max_retries} attempts")
        logger.error("Throwing error for now")
        raise RuntimeError(f"Content not found: {url}. need to start again.")

    def is_webdriver_alive(self):
        logger.info("Checking whether the driver is alive")
        try:
            if self.driver.current_url:
                return True
            logger.info("The driver appears to be alive")
            return False
        except (NoSuchElementException, WebDriverException, AssertionError):
            logger.warning("The driver appears to be dead")
            return False
        except Exception as ex:
            logger.error(
                f"Encountered an unexpected exception type {ex} while checking the driver status"
            )
            return False

    def get_product_items_url(self, url, batch_id):
        logger.info(f"Scraping items for URL: {url}")
        soup = self.get_soup_element(url)
        flag = True
        urls_list = []
        results = []
        title = soup.find("h1", {"id": "page-title-heading"}).text.strip()
        while flag:
            current_urls = get_products_urls(soup)
            logger.info("Current page items url received")
            logger.debug(f"Current page items url received: {current_urls}")

            logger.info(
                f"inserting received urls into products url with batch: {batch_id} urls: {current_urls}"
            )
            batch_id = self.database.create_product_url(current_urls, "psa")
            urls_list.extend(current_urls)
            flag, next_url = is_next_page_available(soup)
            if flag:
                soup = self.get_soup_element(next_url)
                batch_id = self.database.update_check_point(next_url, "psa")
            else:
                break
        logger.debug(f"Obtained product url {urls_list}")
        logger.info(f"Number of obtained product url {len(urls_list)}")
        return urls_list

    def extract_and_save_product_urls(self, url, use_last=True):
        if use_last:
            batch_id, check_point = self.database.fetch_lastest_batch(
                source_code_name="psa"
            )
            url = check_point if check_point else url
            if not batch_id:
                batch_id = self.database.create_batch(source_code_name="psa")
        else:
            batch_id = self.database.create_batch(source_code_name="psa")

        logger.info("Extracting and saving products to the database.")
        url_list = self.get_product_items_url(url, batch_id)

        logger.debug(f"Urls recevied successfully - {url_list}")
        logger.info(f"Urls recevied successfully - {len(url_list)} urls")

        # batch_id = self.database.create_product_url(url_list, "psa")
        logger.info("Product and Urls saved successfully!")
        return "ok"

    def get_url_from_db(self):
        logger.info("Fetching latest batch id from db")
        batch_id, check_point = self.database.fetch_lastest_batch(
            source_code_name="psa"
        )
        logger.info(f"Found batch - {batch_id}")

        logger.info(f"Fetching all urls where is_fetched is false:")
        urls = self.database.fetch_all_urls(batch_id=batch_id)
        logger.debug(f"Got url from product urls table: {urls}")
        logger.info(f"Got url from product urls table: {len(urls)}")
        logger.debug("Returning batchid and urls")
        return batch_id, urls

    def extract_product_from_db_url(self):
        logger.info("Extracting products details from db's url")
        batch_id, urls = self.get_url_from_db()
        for url in urls:
            # format is [(url,), (url,)]
            url = url[0]
            flag = self.is_product_available_in_db(url, source_code_name="psa")
            if not flag:
                logger.info(
                    f"product {url} not found in guns_product, strating scraping"
                )
                soup = self.get_soup_element(url)
                page_not_found = check404(soup)
                if page_not_found:
                    logger.info(f"product {url} page not found, skipping it.")
                    continue
                else:
                    product_details = get_product_data(soup)
                    product_details["product_info"] = {"url": url}
                    filtered_data = self.get_filtered_data(product_details)
                    self.insert_data_to_db(filtered_data)
            else:
                logger.info("Product details already extracted. skipping for now")
            self.database.update_urls(batch_id, url)
        return

    def get_filtered_data(self, result):
        # logger: Start of the method
        logger.info("Getting filtered data.")

        final_output = []
        if isinstance(result, dict):
            result = [result]
        for data in result:
            data_dict = {
                "title": data["title"],
                "selling_price": data["pricing"]["offer_price"],
                "marked_price": data["pricing"]["list_price"],
                "discount_price": data["pricing"]["save_price"],
                "sku": data["attributes"].get("SKU", None),
                "model": data["attributes"].get("Model Number", None),
                "brand": data["attributes"].get("Brand", None),
                "upc": data["attributes"].get("UPC", None),
                "caliber": data["attributes"].get("Caliber", None),
                "description": data["long_description"],
                "availability": data["availability"],
                "features": data["features"],
                "images": data["images"],
                "product_url": data["product_info"].get("url"),
                "category": data["breadcrumb"].get("category", "gun"),
            }
            final_output.append(data_dict)

        # Logging: Log before returning
        logger.info("Returning filtered data.")
        return final_output

    def handle_age_verification(self):
        logger.info("handle_age_verification - checking age verification.")

        # Check for existing cookie
        cookies = self.driver.get_cookies()
        if any(
            cookie["name"] == "psa_age_verification_modal" and cookie["value"] == "1"
            for cookie in cookies
        ):
            logger.info(
                "handle_age_verification - Age verification already handled (cookie found)."
            )
            # Skip verification if cookie is present
            return

        try:
            age_verification_element = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "psa-age-verification-modal")
                )
            )
            logger.info("Age verification appeared..clicking remember me")

            # If visible, perform actions
            remember_me_checkbox = self.driver.find_element(
                By.CLASS_NAME, "age-verification-checkbox-container"
            )
            remember_me_checkbox.click()

            # Click the "Yes" button inside the age verification element
            yes_button = WebDriverWait(age_verification_element, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, './/button[@data-role="action"]/span[text()="Yes"]')
                )
            )
            logger.info("Age verification appeared..clicking yes button")
            yes_button.click()
            time.sleep(2)
            logger.info("Age verification completed.")

        except TimeoutException:
            logger.warning("Age verification not visible or timed out.")
        except Exception as e:
            logger.error(f"An error occurred during age verification: {e}")

        else:
            # Set cookie after successful verification (if not already set)
            cookies = self.driver.get_cookies()

            if not any(
                cookie["name"] == "psa_age_verification_modal" for cookie in cookies
            ):
                logger.info("Adding age verification to cookie")
                # Get the expiration date one month from now
                expiration_date = (
                    datetime.datetime.now() + datetime.timedelta(days=31)
                ).timestamp()

                # Convert expiration date to integer for cookie setting
                expiration_date_int = int(expiration_date)
                self.driver.add_cookie(
                    {
                        "domain": ".palmettostatearmory.com",
                        "expiry": expiration_date_int,
                        "httpOnly": False,
                        "name": "psa_age_verification_modal",
                        "path": "/",
                        "sameSite": "Lax",
                        "secure": True,
                        "value": "1",
                    }
                )
                logger.info("Cookie added successfully!")
            return

    def insert_data_to_db(self, data):
        db = DatabaseLoader()
        logger.info("inserting data to the database")
        db.insert_gun_products(data, source_code_name="psa")

    def is_product_available_in_db(self, url, source_code_name="psa"):
        logger.info("checking for product in db")
        flag = self.database.check_db_for_product(
            url, source_code_name=source_code_name
        )
        return flag
