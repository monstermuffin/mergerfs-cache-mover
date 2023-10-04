import os
import shutil
import logging
from discord_webhook import DiscordWebhook
import configparser
import yaml

# Load configurations from config.yml
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

CACHE_PATH = config['Paths']['CACHE_PATH']
BACKING_PATH = config['Paths']['BACKING_PATH']
LOG_PATH = config['Paths']['LOG_PATH']
THRESHOLD_PERCENTAGE = config['Settings']['THRESHOLD_PERCENTAGE']
USE_WEBHOOK = config['Webhook']['USE_WEBHOOK']
WEBHOOK_URL = config['Webhook']['WEBHOOK_URL']


# Load configurations from settings.ini
config = configparser.ConfigParser()
config.read('settings.ini')

CACHE_PATH = config.get('Paths', 'CACHE_PATH')
BACKING_PATH = config.get('Paths', 'BACKING_PATH')
THRESHOLD_PERCENTAGE = config.getint('Settings', 'THRESHOLD_PERCENTAGE')
USE_WEBHOOK = config.getboolean('Webhook', 'USE_WEBHOOK')
WEBHOOK_URL = config.get('Webhook', 'WEBHOOK_URL')

# Set up logging
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cache_usage(path):
    """Get the usage percentage of the filesystem."""
    stat = os.statvfs(path)
    return (stat.f_blocks - stat.f_bfree) / stat.f_blocks * 100

def find_oldest_file(path):
    """Find the oldest file in the given directory."""
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return min(files, key=os.path.getctime)

def send_webhook_notification(message):
    """Send a notification using a webhook."""
    if not USE_WEBHOOK:
        return

    webhook = DiscordWebhook(url=WEBHOOK_URL, content=message, rate_limit_retry=True)
    try:
        response = webhook.execute()
    except Exception as e:
        logging.error(f"Failed to send webhook notification: {e}")

def main():
    usage = get_cache_usage(CACHE_PATH)
    if usage > THRESHOLD_PERCENTAGE:
        logging.info(f"Cache usage is {usage:.2f}%, exceeding threshold. Starting file move...")
        oldest_file = find_oldest_file(CACHE_PATH)
        try:
            shutil.move(oldest_file, BACKING_PATH)
            logging.info(f"Moved {oldest_file} to {BACKING_PATH}")
            send_webhook_notification(f"Successfully moved {oldest_file} to {BACKING_PATH}")
        except Exception as e:
            logging.error(f"Error moving file: {e}")
            send_webhook_notification(f"Failed to move {oldest_file} to {BACKING_PATH}. Error: {e}")

if __name__ == "__main__":
    main()
