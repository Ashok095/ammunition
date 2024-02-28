import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.getenv("append_path"))

from palmetto_state_armory import ExtractPalmettoStateArmory
from datetime import datetime
import time
from slack import SlackMessenger
import logging

logger = logging.getLogger()


if __name__ == "__main__":
    flag = True
    i = 1
    slack = SlackMessenger()
    while flag:
        current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        slack.send_message(
            f"Info: {current_datetime}: Extracting Palmetto State Armory Products Urls"
        )
        try:
            url = "https://palmettostatearmory.com/guns.html"

            extractor = ExtractPalmettoStateArmory()
            result = extractor.extract_and_save_product_urls(url, use_last=True)
        except Exception as e:
            current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            message = f"{current_datetime}: Error occured while Extracting Palmetto State Armory urls.\nError details: {e}"
            slack.send_message(message)
            sleep_time = i * 5
            slack.send_message(f"sleeping for {sleep_time}seconds")
            time.sleep(sleep_time)
            i += 1
        else:
            current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            message = (
                f"{current_datetime}: Product urls has been successfully extracted"
            )
            slack.send_message(message)
            break
