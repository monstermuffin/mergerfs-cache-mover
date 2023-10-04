from config import CACHE_PATH, BACKING_PATH, PERCENTAGE_THRESHOLD, USE_WEBHOOK, WEBHOOK_URL, MAX_WORKERS
import os
import logging
import shutil
import requests
import concurrent.futures

# Configuration
CACHE_PATH = "/path/to/cache"
BACKING_PATH = "/path/to/cold"
PERCENTAGE_THRESHOLD = 80
WEBHOOK_URL = "some webook ting"
MAX_WORKERS = 5  # Number of concurrent file moves. Adjust based on your system.

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_filesystem_usage(path):
    """Return the filesystem usage percentage for the given path."""
    stats = os.statvfs(path)
    total_space = stats.f_blocks * stats.f_frsize
    free_space = stats.f_bfree * stats.f_frsize
    used_space = total_space - free_space
    usage_percentage = (used_space / total_space) * 100
    return usage_percentage

def find_oldest_file(path):
    """Find and return the path to the oldest file in the given directory."""
    oldest_file = None
    oldest_time = float('inf')

    for root, _, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            access_time = os.path.getatime(file_path)
            if access_time < oldest_time:
                oldest_time = access_time
                oldest_file = file_path

    return oldest_file

def send_webhook_notification(message):
    """Send a notification using a webhook."""
    if not USE_WEBHOOK:
        return

    payload = {"text": message}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to send webhook notification: {e}")


def move_files_concurrently(files_to_move):
    """Move multiple files concurrently."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, src, os.path.join(BACKING_PATH, os.path.basename(src))) for src in files_to_move]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error moving file: {e}")
                send_webhook_notification(f"Error moving file: {e}")

def main():
    # Check if the cache filesystem usage exceeds the threshold
    usage = get_filesystem_usage(CACHE_PATH)
    if usage > PERCENTAGE_THRESHOLD:
        logging.info(f"Cache usage is {usage:.2f}%, exceeding threshold. Starting file move...")
        # Gather a list of files to move
        files_to_move = [find_oldest_file(CACHE_PATH) for _ in range(MAX_WORKERS)]
        move_files_concurrently(files_to_move)
    else:
        logging.info(f"Cache usage is {usage:.2f}%, within acceptable limits.")

if __name__ == "__main__":
    main()
