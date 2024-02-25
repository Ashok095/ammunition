import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.getenv("append_path"))

from midway import MidWayExtractor
from datetime import datetime
import requests
import json
import time
from slack import SlackMessenger

if __name__ == "__main__":
    flag = True
    i = 1
    slack = SlackMessenger()
    while flag:
        current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        slack.send_message(
            f"Info: {current_datetime}: Extracting Midway USA Products Urls"
        )
        try:
            extractor = MidWayExtractor()
            result = extractor.process_algoliasearch()
        except Exception as e:
            current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            message = f"{current_datetime}: Error occured while Extracting Midway USA urls.\nError details: {e}"
            slack.send_message(message)
            sleep_time = i * 5
            slack.send_message(f"sleeping for {sleep_time}seconds")
            time.sleep(sleep_time)
            i += 1
        else:
            current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            message = f"{current_datetime}: Midway USA Product urls has been successfully extracted"
            slack.send_message(message)
            break
