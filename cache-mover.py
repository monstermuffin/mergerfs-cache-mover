import os
import shutil
import stat
import logging
import yaml
import subprocess
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
import argparse
import sys
import psutil
import requests
from threading import Lock, Event
import signal

__version__ = "0.98.7"

class HybridFormatter(logging.Formatter):
    def __init__(self, fmt="%(levelname)s: %(message)s"):
        super().__init__(fmt)

    def format(self, record):
        if hasattr(record, 'file_move'):
            return (f"{self.formatTime(record)} - {record.levelname} - File Move Operation:\n"
                    f"  From: {record.src}\n"
                    f"  To: {record.dest}\n"
                    f"  {record.msg}")
        else:
            return f"{self.formatTime(record)} - {record.levelname} - {record.msg}"

def setup_logging(config, console_log):
    log_formatter = HybridFormatter()
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

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def set_git_dir():
    script_dir = get_script_dir()
    os.environ['GIT_DIR'] = os.path.join(script_dir, '.git')

def get_current_commit_hash():
    set_git_dir()
    try:
        result = subprocess.run(['git', 'rev-parse', 'HEAD'],
                                capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error getting current commit hash: {e}")
        return None

def run_git_command(command, error_message):
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"{error_message} Command: {e.cmd}")
        logging.error(f"Error output: {e.stderr}")
        raise

def auto_update(config):
    set_git_dir()
    current_commit = get_current_commit_hash()
    if not current_commit:
        logging.warning("Unable to get current commit hash. Skipping auto-update.")
        return False

    update_branch = config['Settings'].get('UPDATE_BRANCH', 'main')
    
    try:
        api_url = f"https://api.github.com/repos/MonsterMuffin/mergerfs-cache-mover/commits/{update_branch}"
        response = requests.get(api_url)
        response.raise_for_status()
        latest_commit = response.json()['sha']

        if latest_commit != current_commit:
            logging.info(f"A new version is available on branch '{update_branch}'. Current: {current_commit[:7]}, Latest: {latest_commit[:7]}")
            logging.info("Attempting to auto-update...")

            try:
                run_git_command(['git', 'fetch', 'origin', update_branch],
                                f"Failed to fetch updates from {update_branch}.")
                run_git_command(['git', 'reset', '--hard', f'origin/{update_branch}'],
                                f"Failed to reset to latest commit on {update_branch}.")

                logging.info("Update successful. Restarting script...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
            except subprocess.CalledProcessError:
                return False
        else:
            logging.info(f"Already running the latest version on branch '{update_branch}' (commit: {current_commit[:7]}).")

        return True
    except requests.RequestException as e:
        logging.error(f"Failed to check for updates: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during update process: {e}")
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
    config['Settings']['UPDATE_BRANCH'] = config['Settings'].get('UPDATE_BRANCH', 'main')
    config['Settings']['EXCLUDED_DIRS'] = config['Settings'].get('EXCLUDED_DIRS', [])

    if config['Settings']['THRESHOLD_PERCENTAGE'] <= config['Settings']['TARGET_PERCENTAGE']:
        raise ValueError("THRESHOLD_PERCENTAGE must be greater than TARGET_PERCENTAGE")
    return config

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

def get_fs_free_space(path):
    total, _, free = shutil.disk_usage(path)
    return free

def is_excluded(path, excluded_dirs):
    return any(excluded_dir in path for excluded_dir in excluded_dirs)

def gather_files_to_move(config):
    all_files = []
    excluded_dirs = config['Settings']['EXCLUDED_DIRS']
    
    for dirname, subdirs, filenames in os.walk(config['Paths']['CACHE_PATH']):
        subdirs[:] = [d for d in subdirs if not is_excluded(os.path.join(dirname, d), excluded_dirs)]
        
        if not is_excluded(dirname, excluded_dirs):
            for filename in filenames:
                all_files.append(os.path.join(dirname, filename))

    if not all_files:
        logging.warning("No files found in CACHE_PATH.")
        return []

    all_files.sort(key=lambda fn: os.stat(fn).st_mtime)
    files_to_move = []

    while get_fs_usage(config['Paths']['CACHE_PATH']) > config['Settings']['TARGET_PERCENTAGE'] and all_files:
        files_to_move.append(all_files.pop(0))

    logging.info(f"Total files to move: {len(files_to_move)}")
    return files_to_move

def move_file(src, dest_base, config, target_reached_lock, dry_run=False):
    with target_reached_lock:
        current_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
        if current_usage <= config['Settings']['TARGET_PERCENTAGE']:
            return False, True

    try:
        relative_path = os.path.relpath(src, config['Paths']['CACHE_PATH'])
        dest = os.path.join(dest_base, relative_path)
        dest_dir = os.path.dirname(dest)

        if dry_run:
            logging.info(f"Would move file: {src} to {dest}")
            return True, False

        free_space = get_fs_free_space(dest_base)
        file_size = os.path.getsize(src)
        if free_space < file_size:
            logging.error(f"Not enough space to move file {src}. Required: {file_size}, Available: {free_space}")
            return False, False

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

        logging.info("File moved successfully", extra={'file_move': True, 'src': src, 'dest': dest})
        return True, False
    except Exception as e:
        logging.error(f"Unexpected error moving file: {e}", extra={'file_move': True, 'src': src, 'dest': dest})
        return False, False

def move_files_concurrently(files_to_move, config, dry_run=False, stop_event=None):
    target_reached_lock = Lock()
    files_moved_count = 0
    total_size = sum(os.path.getsize(f) for f in files_to_move)
    
    with ThreadPoolExecutor(max_workers=config['Settings']['MAX_WORKERS']) as executor:
        futures = []
        for src in files_to_move:
            if stop_event and stop_event.is_set():
                logging.info("Graceful shutdown requested. Stopping file moves.")
                break
            future = executor.submit(move_file, src, config['Paths']['BACKING_PATH'], config, target_reached_lock, dry_run)
            futures.append(future)

        target_reached = False
        for future in futures:
            if stop_event and stop_event.is_set():
                break
            success, reached = future.result()
            if success:
                files_moved_count += 1
            if reached and not target_reached:
                logging.info(f"Target percentage reached. Stopping new file moves.")
                target_reached = True
                break

    final_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
    logging.info(f"File move {'simulation' if dry_run else 'operation'} complete. Final cache usage: {final_usage:.2f}%")
    return files_moved_count

def remove_empty_dirs(path, excluded_dirs, dry_run=False):
    empty_dirs_count = 0
    for root, dirs, files in os.walk(path, topdown=False):
        dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d), excluded_dirs)]
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                if dry_run:
                    logging.info(f"Would remove empty directory: {dir_path}")
                    empty_dirs_count += 1
                else:
                    try:
                        os.rmdir(dir_path)
                        logging.info(f"Removed empty directory: {dir_path}")
                        empty_dirs_count += 1
                    except OSError as e:
                        logging.error(f"Error removing directory {dir_path}: {e}")
    
    if dry_run:
        if empty_dirs_count == 0:
            logging.info("No empty directories found to remove")
        else:
            logging.info(f"Dry run: Would remove {empty_dirs_count} empty directories")
    else:
        if empty_dirs_count == 0:
            logging.info("No empty directories were removed")
        else:
            logging.info(f"Removed {empty_dirs_count} empty directories")

    return empty_dirs_count

def main():
    parser = argparse.ArgumentParser(description='Move files from cache to backing storage.')
    parser.add_argument('--console-log', action='store_true', help='Display logs in the console.')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without actually moving files.')
    args = parser.parse_args()

    try:
        config = load_config()
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)

    logger = setup_logging(config, args.console_log)

    script_dir = get_script_dir()
    logging.info(f"Script directory: {script_dir}")
    logging.info(f"Current working directory: {os.getcwd()}")

    running, processes = is_script_running()
    if running:
        for process in processes:
            logging.warning(f"Detected process: {process}")
        logging.warning("Another instance of the script is running. Exiting.")
        return

    if config['Settings'].get('AUTO_UPDATE', True):
        logging.info(f"Auto-update is enabled. Using branch: {config['Settings']['UPDATE_BRANCH']}")
        if not auto_update(config):
            logging.warning("Proceeding with current version due to update failure or no updates available.")
    else:
        logging.info("Auto-update is disabled. Skipping update check.")

    current_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
    logging.info(f"Current cache usage: {current_usage:.2f}%")
    logging.info(f"Threshold percentage: {config['Settings']['THRESHOLD_PERCENTAGE']}%")
    logging.info(f"Target percentage: {config['Settings']['TARGET_PERCENTAGE']}%")

    stop_event = Event()
    def signal_handler(signum, frame):
        logging.info(f"Received signal {signum}. Initiating graceful shutdown.")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if current_usage > config['Settings']['THRESHOLD_PERCENTAGE']:
        logging.info(f"Cache usage is {current_usage:.2f}%, exceeding threshold. Starting file move...")
        files_to_move = gather_files_to_move(config)
        
        if args.dry_run:
            logging.info("Dry run mode. Simulating file moves and directory cleanup:")
            moved_files_count = move_files_concurrently(files_to_move, config, dry_run=True, stop_event=stop_event)
            empty_dirs_count = remove_empty_dirs(config['Paths']['CACHE_PATH'], config['Settings']['EXCLUDED_DIRS'], dry_run=True)
            
            logging.info("Dry run summary:")
            logging.info(f"- Would move {moved_files_count} files")
            logging.info(f"- Would remove {empty_dirs_count} empty directories")
        else:
            logging.info("Starting actual file move operation:")
            moved_files_count = move_files_concurrently(files_to_move, config, dry_run=False, stop_event=stop_event)
            if moved_files_count > 0:
                logging.info(f"Moved {moved_files_count} files. Performing cleanup of empty directories.")
                empty_dirs_count = remove_empty_dirs(config['Paths']['CACHE_PATH'], config['Settings']['EXCLUDED_DIRS'])
                logging.info(f"Removed {empty_dirs_count} empty directories")
            else:
                logging.info("No files were moved. Skipping directory cleanup.")
            
            logging.info("Operation summary:")
            logging.info(f"- Moved {moved_files_count} files")
            logging.info(f"- Removed {empty_dirs_count} empty directories")
    else:
        logging.info(f"Cache usage is {current_usage:.2f}%, below the threshold ({config['Settings']['THRESHOLD_PERCENTAGE']}%). No action required.")

if __name__ == "__main__":
    main()