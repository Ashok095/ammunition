import psutil
import logging 
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.getenv('append_path'))


logger = logging.getLogger()
import subprocess


def kill_chrome_process():
    # Iterate over all running processes
    for process in psutil.process_iter():
        try:
            # Check if the process name contains 'chrome'
            if 'chrom' in process.name().lower():
                # Terminate the process
                process.terminate()
                logger.warning("Chrome process terminated successfully.")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass



def open_chrome():
    try:
        subprocess.Popen(['chromium'])  # Command to open Chrome
        logger.info("Chrome opened successfully.")
    except Exception as e:
        logger("Error:", e)

