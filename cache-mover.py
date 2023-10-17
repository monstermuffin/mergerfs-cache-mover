import os
import shutil
import logging
import yaml
import subprocess
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler

# Load configurations from config.yml
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

CACHE_PATH = config['Paths']['CACHE_PATH']
BACKING_PATH = config['Paths']['BACKING_PATH']
LOG_PATH = config['Paths']['LOG_PATH']
THRESHOLD_PERCENTAGE = config['Settings']['THRESHOLD_PERCENTAGE']
MAX_WORKERS = config['Settings']['MAX_WORKERS']
MAX_LOG_SIZE_MB = config['Settings']['MAX_LOG_SIZE_MB']
BACKUP_COUNT = config['Settings']['BACKUP_COUNT']

# Convert log size from MB to bytes
MAX_LOG_SIZE_BYTES = MAX_LOG_SIZE_MB * 1024 * 1024

# Set up logging with rotation
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(LOG_PATH, maxBytes=MAX_LOG_SIZE_BYTES, backupCount=BACKUP_COUNT)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

def get_fs_usage(path):
    total, used, free = shutil.disk_usage(path)
    return (used / total) * 100

def gather_files_to_move():
    all_files = [
        os.path.join(dirname, filename)
        for dirname, _, filenames in os.walk(CACHE_PATH)
        for filename in filenames
    ]

    if not all_files:
        logging.warning("No files found in CACHE_PATH.")
        return []

    all_files.sort(key=lambda fn: os.stat(fn).st_mtime)
    files_to_move = []

    while get_fs_usage(CACHE_PATH) > THRESHOLD_PERCENTAGE and all_files:
        oldest_file = all_files.pop(0)
        files_to_move.append(oldest_file)
    return files_to_move

def move_file(src, dest):
    # Move a file using rsync and log the action.
    try:
        cmd = ["rsync", "-avh", "--remove-source-files", src, dest]
        subprocess.check_call(cmd)
        logger.info(f"Moved {src} to {dest}")
    except subprocess.CalledProcessError:
        logger.error(f"Error moving file using rsync.")
    except Exception as e:
        logger.error(f"Unexpected error moving file: {e}")

def move_files_concurrently(files_to_move):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, src, BACKING_PATH) for src in files_to_move]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error moving file: {e}")

def delete_empty_dirs(path):
    """Recursively delete empty directories."""
    # If the path itself is not a directory, exit.
    if not os.path.isdir(path):
        return

    # Remove child directories first.
    for child_dir in [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]:
        delete_empty_dirs(os.path.join(path, child_dir))

    # Check if the directory is empty.
    if not os.listdir(path) and path not in [os.path.join(CACHE_PATH, d) for d in os.listdir(CACHE_PATH) if os.path.isdir(os.path.join(CACHE_PATH, d))]:
        logging.info(f"Removed empty directory: {path}")
        os.rmdir(path)

def main():
    current_usage = get_fs_usage(CACHE_PATH)
    if current_usage > THRESHOLD_PERCENTAGE:
        logging.info(f"Cache usage is {current_usage:.2f}%, exceeding threshold. Starting file move...")
        files_to_move = gather_files_to_move()
        move_files_concurrently(files_to_move)

    # Clean up any empty directories under the cache path
    for root_folder in [os.path.join(CACHE_PATH, d) for d in os.listdir(CACHE_PATH) if os.path.isdir(os.path.join(CACHE_PATH, d))]:
        delete_empty_dirs(root_folder)

if __name__ == "__main__":
    main()