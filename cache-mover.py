import os
import shutil
import stat
import logging
import yaml
import subprocess
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
import argparse
import requests
import sys
import psutil

__version__ = "0.96.5"

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_current_commit_hash():
    script_dir = get_script_dir()
    try:
        return subprocess.check_output(['git', '-C', script_dir, 'rev-parse', 'HEAD'], stderr=subprocess.DEVNULL).decode('ascii').strip()
    except subprocess.CalledProcessError:
        return None

def auto_update(config):
    if not config['Settings'].get('AUTO_UPDATE', True):
        logging.info("Auto-update is disabled.")
        return False

    current_commit = get_current_commit_hash()
    if not current_commit:
        logging.warning("Unable to get current commit hash. Make sure this is a git repository.")
        return False

    try:
        api_url = "https://api.github.com/repos/MonsterMuffin/mergerfs-cache-mover/commits/main"
        response = requests.get(api_url)
        response.raise_for_status()
        latest_commit = response.json()['sha']

        if latest_commit != current_commit:
            logging.info(f"A new version is available. Current: {current_commit[:7]}, Latest: {latest_commit[:7]}")
            logging.info("Attempting to auto-update...")

            try:
                script_dir = get_script_dir()
                subprocess.check_call(['git', '-C', script_dir, 'fetch', 'origin', 'main'], stderr=subprocess.DEVNULL)
                subprocess.check_call(['git', '-C', script_dir, 'reset', '--hard', 'origin/main'], stderr=subprocess.DEVNULL)
                logging.info("Update successful. Restarting script...")

                os.execv(sys.executable, [sys.executable] + sys.argv)
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to update: {e}")
                return False
        else:
            logging.info(f"Already running the latest version (commit: {current_commit[:7]}).")

        return True
    except Exception as e:
        logging.error(f"Failed to check for updates: {e}")
        return False

def load_config():
    script_dir = get_script_dir()
    config_path = os.path.join(script_dir, 'config.yml')
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)

    config['Settings']['THRESHOLD_PERCENTAGE'] = float(config['Settings']['THRESHOLD_PERCENTAGE'])
    config['Settings']['TARGET_PERCENTAGE'] = float(config['Settings']['TARGET_PERCENTAGE'])
    config['Settings']['MAX_WORKERS'] = int(config['Settings']['MAX_WORKERS'])
    config['Settings']['MAX_LOG_SIZE_MB'] = int(config['Settings']['MAX_LOG_SIZE_MB'])
    config['Settings']['BACKUP_COUNT'] = int(config['Settings']['BACKUP_COUNT'])
    config['Settings']['AUTO_UPDATE'] = config['Settings'].get('AUTO_UPDATE', True)

    return config

def setup_logging(config, console_log):
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler = RotatingFileHandler(
        config['Paths']['LOG_PATH'],
        maxBytes=config['Settings']['MAX_LOG_SIZE_MB'] * 1024 * 1024,
        backupCount=config['Settings']['BACKUP_COUNT']
    )
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    if console_log:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger

def is_script_running():
    current_process = psutil.Process()
    current_script = os.path.abspath(__file__)
    script_name = os.path.basename(current_script)
    
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        if process.pid != current_process.pid:
            try:
                if process.name() == 'python' or process.name() == 'python3':
                    cmdline = process.cmdline()
                    if len(cmdline) >= 2 and script_name in cmdline[-1]:
                        if not is_child_process(current_process, process):
                            return True, [' '.join(cmdline)]
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    return False, []

def is_child_process(parent, child):
    try:
        return child.ppid() == parent.pid
    except psutil.NoSuchProcess:
        return False

def get_fs_usage(path):
    total, used, _ = shutil.disk_usage(path)
    return (used / total) * 100

def gather_files_to_move(config):
    all_files = [
        os.path.join(dirname, filename)
        for dirname, _, filenames in os.walk(config['Paths']['CACHE_PATH'])
        for filename in filenames
    ]

    if not all_files:
        logging.warning("No files found in CACHE_PATH.")
        return []

    all_files.sort(key=lambda fn: os.stat(fn).st_mtime)
    files_to_move = []

    while get_fs_usage(config['Paths']['CACHE_PATH']) > config['Settings']['TARGET_PERCENTAGE'] and all_files:
        files_to_move.append(all_files.pop(0))
        if len(files_to_move) % 10 == 0:
            current_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
            logging.info(f"Current cache usage: {current_usage:.2f}%")
            if current_usage <= config['Settings']['TARGET_PERCENTAGE']:
                logging.info(f"Reached target percentage. Stopping file gathering.")
                break

    return files_to_move

def move_file(src, dest_base, config):
    try:
        current_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
        if current_usage <= config['Settings']['TARGET_PERCENTAGE']:
            logging.info(f"Reached target percentage ({current_usage:.2f}%). Skipping further file moves.")
            return False

        relative_path = os.path.relpath(src, config['Paths']['CACHE_PATH'])
        dest = os.path.join(dest_base, relative_path)
        dest_dir = os.path.dirname(dest)

        os.makedirs(dest_dir, exist_ok=True)
        src_stat = os.stat(src)
        if src_stat.st_nlink > 1:
            subprocess.run(['cp', '-al', src, dest], check=True)
        else:
            shutil.copy2(src, dest)
        os.chmod(dest, stat.S_IMODE(src_stat.st_mode))
        try:
            os.chown(dest, src_stat.st_uid, src_stat.st_gid)
        except PermissionError:
            logging.warning(f"Unable to change ownership of {dest}. This may require root privileges.")

        os.remove(src)

        logging.info(f"Moved {src} to {dest}")
        return True
    except Exception as e:
        logging.error(f"Unexpected error moving file from {src} to {dest}: {e}")
        return False

def move_files_concurrently(files_to_move, config):
    with ThreadPoolExecutor(max_workers=config['Settings']['MAX_WORKERS']) as executor:
        futures = []
        for src in files_to_move:
            if get_fs_usage(config['Paths']['CACHE_PATH']) <= config['Settings']['TARGET_PERCENTAGE']:
                logging.info(f"Reached target percentage. Stopping file move.")
                break
            futures.append(executor.submit(move_file, src, config['Paths']['BACKING_PATH'], config))

        for future in futures:
            if not future.result():
                break

    final_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
    logging.info(f"File move complete. Final cache usage: {final_usage:.2f}%")

    remove_empty_dirs(config['Paths']['CACHE_PATH'])

def remove_empty_dirs(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                try:
                    os.rmdir(dir_path)
                    logging.info(f"Removed empty directory: {dir_path}")
                except OSError as e:
                    logging.error(f"Error removing directory {dir_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Move files from cache to backing storage.')
    parser.add_argument('--console-log', action='store_true', help='Display logs in the console.')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without actually moving files.')
    args = parser.parse_args()

    config = load_config()
    logger = setup_logging(config, args.console_log)

    running, processes = is_script_running()
    if running:
        for process in processes:
            logger.warning(f"Detected process: {process}")
        logger.warning("Another instance of the script is running. Exiting.")
        return

    if config['Settings'].get('AUTO_UPDATE', True):
        if not auto_update(config):
            logging.warning("Proceeding with current version due to update failure or no updates available.")
    else:
        logging.info("Auto-update is disabled. Skipping update check.")

    current_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
    logger.info(f"Current cache usage: {current_usage:.2f}%")
    logger.info(f"Threshold percentage: {config['Settings']['THRESHOLD_PERCENTAGE']}%")
    logger.info(f"Target percentage: {config['Settings']['TARGET_PERCENTAGE']}%")

    if current_usage > config['Settings']['THRESHOLD_PERCENTAGE']:
        logger.info(f"Cache usage is {current_usage:.2f}%, exceeding threshold. Starting file move...")
        files_to_move = gather_files_to_move(config)
        if args.dry_run:
            logger.info("Dry run mode. The following files would be moved:")
            for file in files_to_move:
                logger.info(f"Would move: {file}")
        else:
            move_files_concurrently(files_to_move, config)
    else:
        logger.info(f"Cache usage is {current_usage:.2f}%, below the threshold ({config['Settings']['THRESHOLD_PERCENTAGE']}%). No action required.")

if __name__ == "__main__":
    main()