import requests
import json
import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.getenv('append_path'))

logger = logging.getLogger()


class SlackMessenger:
    def __init__(self):
        self.webhook_url = os.getenv("webhook_url")
        self.header = {"Content-type": "application/json"}
        self.payload = {'text': ""}

    def send_message(self, msg):
        self.payload['text'] = msg
        data = json.dumps(self.payload)
        logger.info(f"Slack: {msg}")
        response = requests.post(self.webhook_url, headers=self.header, data=data)
        return response.text

