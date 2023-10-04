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

def move_file(src, dest):
    """Move a file from src to dest."""
    shutil.move(src, dest)
    logging.info(f"Moved {src} to {dest}")

def send_webhook_notification(message):
    """Send a notification using a webhook."""
    payload = {"text": message}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to send webhook notification: {e}")

def main():
    # Check if the cache filesystem usage exceeds the threshold
    usage = get_filesystem_usage(CACHE_PATH)
    if usage > PERCENTAGE_THRESHOLD:
        logging.info(f"Cache usage is {usage:.2f}%, exceeding threshold. Starting file move...")
        # Find the oldest file
        file_to_move = find_oldest_file(CACHE_PATH)
        if file_to_move:
            # Check if there's enough space in the backing filesystem
            file_size = os.path.getsize(file_to_move)
            backing_free_space = os.statvfs(BACKING_PATH).f_bfree * os.statvfs(BACKING_PATH).f_frsize
            if file_size <= backing_free_space:
                # Move the file to the backing filesystem
                move_file(file_to_move, os.path.join(BACKING_PATH, os.path.basename(file_to_move)))
            else:
                message = "Not enough space in the backing filesystem to move the file."
                logging.warning(message)
                send_webhook_notification(message)
        else:
            logging.warning("No files found to move.")
    else:
        logging.info(f"Cache usage is {usage:.2f}%, within acceptable limits.")

if __name__ == "__main__":
    main()