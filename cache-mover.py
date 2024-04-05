#Muffin's mergerfs cache mover. https://github.com/MonsterMuffin/mergerfs-cache-mover

import os
import shutil
import logging
import yaml
import subprocess
import signal
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
import argparse

# Add command-line arguments
parser = argparse.ArgumentParser(description='Move files from cache to backing storage.')
parser.add_argument('--console-log', action='store_true', help='Display logs in the console.')
args = parser.parse_args()

# Get the absolute path to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.yml')

# Load configurations from config.yml
with open(config_path, 'r') as config_file:
    config = yaml.safe_load(config_file)

CACHE_PATH = config['Paths']['CACHE_PATH']
BACKING_PATH = config['Paths']['BACKING_PATH']
LOG_PATH = config['Paths']['LOG_PATH']
THRESHOLD_PERCENTAGE = float(config['Settings']['THRESHOLD_PERCENTAGE'])
TARGET_PERCENTAGE = float(config['Settings']['TARGET_PERCENTAGE'])
MAX_WORKERS = int(config['Settings']['MAX_WORKERS'])
MAX_LOG_SIZE_MB = int(config['Settings']['MAX_LOG_SIZE_MB'])
BACKUP_COUNT = int(config['Settings']['BACKUP_COUNT'])
USER = config['Settings']['USER']
GROUP = config['Settings']['GROUP']
FILE_CHMOD = config['Settings']['FILE_CHMOD']
DIR_CHMOD = config['Settings']['DIR_CHMOD']

# Convert log size from MB to bytes
MAX_LOG_SIZE_BYTES = MAX_LOG_SIZE_MB * 1024 * 1024

# Set up logging with rotation
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(LOG_PATH, maxBytes=MAX_LOG_SIZE_BYTES, backupCount=BACKUP_COUNT)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Add a StreamHandler to log to the console if --console-log is specified
if args.console_log:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

def signal_handler(signal, frame):
    logger.info("Received SIGINT signal. Waiting for the current file transfer to complete before exiting.")
    # Set a flag to indicate that the script should exit after the current file transfer
    global should_exit
    should_exit = True

signal.signal(signal.SIGINT, signal_handler)

def is_script_running():
    current_pid = os.getpid()  # Getting the PID of the current script
    try:
        cmd = f"pgrep -fl {os.path.basename(__file__)}"
        output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')
        relevant_processes = [line for line in output if str(current_pid) not in line and os.path.basename(__file__) in line]
        pids = [line.split()[0] for line in relevant_processes]
        commands = [" ".join(line.split()[1:]) for line in relevant_processes]

        if pids:
            return True, commands
        else:
            return False, []
    except subprocess.CalledProcessError:
        return False, []
    except Exception as e:
        logging.error(f"Error checking for running script instances: {e}")
        return False, []

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

    # Use TARGET_PERCENTAGE to determine when to stop moving files
    while get_fs_usage(CACHE_PATH) > TARGET_PERCENTAGE and all_files:
        oldest_file = all_files.pop(0)
        files_to_move.append(oldest_file)
    return files_to_move


def move_file(src, dest_base):
    # Move a file using rsync and log the action
    try:
        # Get the relative path of the source file with respect to CACHE_PATH
        relative_path = os.path.relpath(src, CACHE_PATH)

        # Construct the full destination directory
        dest_dir = os.path.join(dest_base, os.path.dirname(relative_path))

        # Ensure the destination directory exists
        os.makedirs(dest_dir, exist_ok=True)

        cmd = ["rsync", "-avh", "--remove-source-files", f"--chown={USER}:{GROUP}", f"--chmod={FILE_CHMOD}", "--perms", f"--chmod=D{DIR_CHMOD}", src, dest_dir]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Error moving file from {src} to {dest_dir} using rsync. Return code: {result.returncode}. Output: {result.stdout}. Error: {result.stderr}")
        else:
            logger.info(f"Moved {src} to {os.path.join(dest_dir, os.path.basename(src))}")
    except subprocess.CalledProcessError as cpe:
        logger.error(f"Error moving file from {src} to {dest_dir} using rsync. Error: {cpe}")
    except Exception as e:
        logger.error(f"Unexpected error moving file from {src} to {dest_dir}: {e}")

def move_files_concurrently(files_to_move):
    global should_exit
    should_exit = False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, src, BACKING_PATH) for src in files_to_move]
        for future in futures:
            if should_exit:
                logger.info("Exiting after the current file transfer completes.")
                break
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error moving file: {e}")

def delete_empty_dirs(path):
    # Recursively delete empty directories.
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
    running, processes = is_script_running()
    if running:
        for process in processes:
            logging.warning(f"Detected process: {process}")
        logging.warning("Another instance of the script is running. Exiting.")
        return

    current_usage = get_fs_usage(CACHE_PATH)
    logging.debug(f"Current cache usage: {current_usage:.2f}%")
    logging.debug(f"Threshold percentage: {THRESHOLD_PERCENTAGE}%")

    if current_usage > THRESHOLD_PERCENTAGE:
        logging.info(f"Cache usage is {current_usage:.2f}%, exceeding threshold. Starting file move...")
        files_to_move = gather_files_to_move()
        move_files_concurrently(files_to_move)
    else:
        logging.info(f"Cache usage is below the threshold ({THRESHOLD_PERCENTAGE}%). No action required.")

    # Clean up any empty directories under the cache path
    for root_folder in [os.path.join(CACHE_PATH, d) for d in os.listdir(CACHE_PATH) if os.path.isdir(os.path.join(CACHE_PATH, d))]:
        delete_empty_dirs(root_folder)

if __name__ == "__main__":
    main()
