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

def is_script_running():
    try:
        # Use ps and grep to count instances of the script running
        cmd = f"ps aux | grep {os.path.basename(__file__)} | grep -v grep | wc -l"
        count = int(subprocess.check_output(cmd, shell=True).strip())

        # If count > 1, then another instance of the script is running
        # (The current instance is also counted.)
        return count > 1
    except Exception as e:
        logging.error(f"Error checking for running script instances: {e}")
        return False

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
        logger.info(f"Moved {src} to {dest}")
    except Exception as e:
        logger.error(f"Error moving file: {e}")

def move_files_concurrently(files_to_move):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, src, os.path.join(BACKING_PATH, os.path.basename(src))) for src in files_to_move]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error moving file: {e}")

def main():
    if is_script_running():
        logging.warning("Another instance of the script is running. Exiting.")
        return

    current_usage = get_fs_usage(CACHE_PATH)
    if current_usage > THRESHOLD_PERCENTAGE:
        logging.info(f"Cache usage is {current_usage:.2f}%, exceeding threshold. Starting file move...")
        files_to_move = gather_files_to_move()
        move_files_concurrently(files_to_move)

if __name__ == "__main__":
    main()