import os
import shutil
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor
from discord_webhook import DiscordWebhook

# Load configurations from config.yml
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

CACHE_PATH = config['Paths']['CACHE_PATH']
BACKING_PATH = config['Paths']['BACKING_PATH']
LOG_PATH = config['Paths']['LOG_PATH']
THRESHOLD_PERCENTAGE = config['Settings']['THRESHOLD_PERCENTAGE']
MAX_WORKERS = config['Settings']['MAX_WORKERS']
USE_WEBHOOK = config['Webhook']['USE_WEBHOOK']
WEBHOOK_URL = config['Webhook']['WEBHOOK_URL']

# Set up logging
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_fs_usage(path):
    """Get filesystem usage percentage."""
    total, used, free = shutil.disk_usage(path)
    return (used / total) * 100

def gather_files_to_move():
    """Gather files to be moved until the threshold is no longer exceeded."""
    files_to_move = []
    while get_fs_usage(CACHE_PATH) > THRESHOLD_PERCENTAGE:
        oldest_file = min(
            (os.path.join(dirname, filename)
            for dirname, _, filenames in os.walk(CACHE_PATH)
            for filename in filenames),
            key=lambda fn: os.stat(fn).st_mtime)
        files_to_move.append(oldest_file)
    return files_to_move

def move_file(src, dest):
    """Move a file and log the action."""
    try:
        shutil.move(src, dest)
        logging.info(f"Moved {src} to {dest}")
        if USE_WEBHOOK:
            webhook = DiscordWebhook(url=WEBHOOK_URL, content=f"Moved {src} to {dest}")
            webhook.execute()
    except Exception as e:
        logging.error(f"Error moving file: {e}")

def move_files_concurrently(files_to_move):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, src, os.path.join(BACKING_PATH, os.path.basename(src))) for src in files_to_move]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error moving file: {e}")

def main():
    current_usage = get_fs_usage(CACHE_PATH)
    if current_usage > THRESHOLD_PERCENTAGE:
        logging.info(f"Cache usage is {current_usage:.2f}%, exceeding threshold. Starting file move...")
        files_to_move = gather_files_to_move()
        move_files_concurrently(files_to_move)

if __name__ == "__main__":
    main()