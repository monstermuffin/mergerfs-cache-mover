import os
import shutil
import stat
import logging
import yaml
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
import argparse
import sys
import psutil
import requests
from threading import Lock, Event
import signal
from time import time
from notifications import NotificationHandler

__version__ = "1.1"

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
    # Hardcode snapraid-related exclusions because reasons
    HARDCODED_EXCLUSIONS = [
        'snapraid',
        '.snapraid',
        '.content'
    ]

    default_config = {
        'Paths': {
            'LOG_PATH': '/var/log/cache-mover.log'
        },
        'Settings': {
            'AUTO_UPDATE': False,
            'THRESHOLD_PERCENTAGE': 70,
            'TARGET_PERCENTAGE': 25,
            'MAX_WORKERS': 8,
            'MAX_LOG_SIZE_MB': 100,
            'BACKUP_COUNT': 1,
            'UPDATE_BRANCH': 'main',
            'EXCLUDED_DIRS': HARDCODED_EXCLUSIONS,
            'SCHEDULE': '0 3 * * *',
            'NOTIFICATIONS_ENABLED': False,  
            'NOTIFICATION_URLS': [],
            'NOTIFY_THRESHOLD': False
        }
    }

    script_dir = get_script_dir()
    config_path = os.path.join(script_dir, 'config.yml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as config_file:
            file_config = yaml.safe_load(config_file)
            default_config['Paths'].update(file_config.get('Paths', {}))
            
            user_exclusions = file_config.get('Settings', {}).get('EXCLUDED_DIRS') or []
            combined_exclusions = list(set(HARDCODED_EXCLUSIONS + user_exclusions))  # merge exclusions
            
            settings_update = file_config.get('Settings', {})
            settings_update['EXCLUDED_DIRS'] = combined_exclusions
            default_config['Settings'].update(settings_update)

    env_mappings = {
        'CACHE_PATH': ('Paths', 'CACHE_PATH'),
        'BACKING_PATH': ('Paths', 'BACKING_PATH'),
        'LOG_PATH': ('Paths', 'LOG_PATH'),
        'THRESHOLD_PERCENTAGE': ('Settings', 'THRESHOLD_PERCENTAGE', float),
        'TARGET_PERCENTAGE': ('Settings', 'TARGET_PERCENTAGE', float),
        'MAX_WORKERS': ('Settings', 'MAX_WORKERS', int),
        'MAX_LOG_SIZE_MB': ('Settings', 'MAX_LOG_SIZE_MB', int),
        'BACKUP_COUNT': ('Settings', 'BACKUP_COUNT', int),
        'UPDATE_BRANCH': ('Settings', 'UPDATE_BRANCH', str),
        'EXCLUDED_DIRS': ('Settings', 'EXCLUDED_DIRS', lambda x: list(set(HARDCODED_EXCLUSIONS + (x.split(',') if x else [])))),
        'SCHEDULE': ('Settings', 'SCHEDULE', str),
        'NOTIFICATIONS_ENABLED': ('Settings', 'NOTIFICATIONS_ENABLED', lambda x: x.lower() == 'true'),
        'NOTIFICATION_URLS': ('Settings', 'NOTIFICATION_URLS', lambda x: x.split(',')),
        'NOTIFY_THRESHOLD': ('Settings', 'NOTIFY_THRESHOLD', lambda x: x.lower() == 'false')
    }

    for env_var, (section, key, *convert) in env_mappings.items():
        env_value = os.environ.get(env_var)
        if env_value is not None:
            if convert:
                env_value = convert[0](env_value)
            default_config[section][key] = env_value

    required_paths = ['CACHE_PATH', 'BACKING_PATH']
    missing_paths = [path for path in required_paths 
                    if not default_config['Paths'].get(path)]
    
    if missing_paths:
        raise ValueError(f"Required paths not configured: {', '.join(missing_paths)}. "
                        f"Please set via config.yml or environment variables.")

    if os.environ.get('DOCKER_CONTAINER'):
        default_config['Settings']['AUTO_UPDATE'] = False
        default_config['Settings']['MAX_LOG_SIZE_MB'] = 100
        default_config['Settings']['BACKUP_COUNT'] = 1

    threshold = default_config['Settings']['THRESHOLD_PERCENTAGE']
    target = default_config['Settings']['TARGET_PERCENTAGE']
    
    # empty mode when both 0 w/ log
    if threshold == 0 and target == 0:
        logging.info("Both THRESHOLD_PERCENTAGE and TARGET_PERCENTAGE are 0. Cache will be emptied completely.")
    # else ensure threshold > target
    elif threshold <= target:
        raise ValueError("THRESHOLD_PERCENTAGE must be greater than TARGET_PERCENTAGE (or both must be 0 to empty cache completely)")

    return default_config

def is_script_running():
    current_process = psutil.Process()
    current_script = os.path.abspath(__file__)
    script_name = os.path.basename(current_script)
    
    # docker check for process inside container
    if os.environ.get('DOCKER_CONTAINER'):
        container_processes = [p for p in psutil.process_iter(['pid', 'name', 'cmdline']) 
                             if p.pid != current_process.pid]
        running_instances = []
        
        for process in container_processes:
            try:
                if process.name() == 'python' or process.name() == 'python3':
                    cmdline = process.cmdline()
                    if len(cmdline) >= 2 and script_name in cmdline[-1]:
                        if not is_child_process(current_process, process):
                            running_instances.append(' '.join(cmdline))
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
                
        return bool(running_instances), running_instances
    
    # non-docker env
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

def _format_bytes(bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.2f}{unit}"
        bytes /= 1024
    return f"{bytes:.2f}PB"

def is_excluded(path, excluded_dirs):
    path_lower = path.lower()
    filename = os.path.basename(path_lower)
    
    # check file patterns for content files
    if filename.endswith('.content'):
        return True
    
    # check dir patterns for snapraid
    path_parts = path_lower.split(os.sep)
    return any(excluded.lower() in path_parts for excluded in excluded_dirs)

def gather_files_to_move(config):
    all_files = []
    excluded_dirs = config['Settings']['EXCLUDED_DIRS']
    
    logging.info(f"Exclusion patterns active: {', '.join(excluded_dirs)}")
    
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

    # Add empty mode when both = 0
    if config['Settings']['THRESHOLD_PERCENTAGE'] == 0 and config['Settings']['TARGET_PERCENTAGE'] == 0:
        logging.info("Moving all files from cache (empty cache mode)")
        return all_files
    
    # else continue as normal
    while get_fs_usage(config['Paths']['CACHE_PATH']) > config['Settings']['TARGET_PERCENTAGE'] and all_files:
        files_to_move.append(all_files.pop(0))

    logging.info(f"Total files to move: {len(files_to_move)}")
    return files_to_move

def move_file(src, dest_base, config, target_reached_lock, dry_run=False, stop_event=None):
    if stop_event and stop_event.is_set():
        return False, False

    with target_reached_lock:
        current_usage = get_fs_usage(config['Paths']['CACHE_PATH'])
        if current_usage <= config['Settings']['TARGET_PERCENTAGE']:
            return False, True

    try:
        relative_path = os.path.relpath(src, config['Paths']['CACHE_PATH'])
        dest = os.path.join(dest_base, relative_path)
        dest_dir = os.path.dirname(dest)

        if dry_run:
            logging.info("Would move file", extra={'file_move': True, 'src': src, 'dest': dest})
            return True, False

        free_space = get_fs_free_space(dest_base)
        file_size = os.path.getsize(src)
        if free_space < file_size:
            logging.error(f"Not enough space to move file", extra={'file_move': True, 'src': src, 'dest': dest})
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
            logging.warning(f"Unable to change ownership", extra={'file_move': True, 'src': src, 'dest': dest})

        if stop_event and stop_event.is_set():
            return False, False

        os.remove(src)

        logging.info("File moved successfully", extra={'file_move': True, 'src': src, 'dest': dest})
        return True, False
    except Exception as e:
        logging.error(f"Unexpected error moving file: {str(e)}", extra={'file_move': True, 'src': src, 'dest': dest})
        return False, False

def move_files_concurrently(files_to_move, config, dry_run=False, stop_event=None):
    target_reached_lock = Lock()
    files_moved_count = 0
    total_bytes = sum(os.path.getsize(f) for f in files_to_move)
    start_time = time()

    with ThreadPoolExecutor(max_workers=config['Settings']['MAX_WORKERS']) as executor:
        futures = []
        for src in files_to_move:
            if stop_event and stop_event.is_set():
                logging.info("Graceful shutdown requested. Stopping new file moves.")
                break
            future = executor.submit(move_file, src, config['Paths']['BACKING_PATH'], config, target_reached_lock, dry_run, stop_event)
            futures.append(future)

        target_reached = False
        for future in as_completed(futures):
            if stop_event and stop_event.is_set():
                logging.info("Graceful shutdown in progress. Waiting for current moves to complete.")
                executor.shutdown(wait=False)
                break
            success, reached = future.result()
            if success:
                files_moved_count += 1
            if reached and not target_reached:
                logging.info(f"Target percentage reached. Stopping new file moves.")
                target_reached = True
                executor.shutdown(wait=False)
                break

    elapsed_time = time() - start_time
    cache_total, cache_used, cache_free = shutil.disk_usage(config['Paths']['CACHE_PATH'])
    final_cache_usage = (cache_used / cache_total) * 100
    
    backing_total, backing_used, backing_free = shutil.disk_usage(config['Paths']['BACKING_PATH'])
    final_backing_usage = (backing_used / backing_total) * 100

    logging.info(f"File move {'simulation' if dry_run else 'operation'} complete.")
    logging.info(f"Cache usage: {final_cache_usage:.2f}% ({_format_bytes(cache_free)} free of {_format_bytes(cache_total)} total)")
    logging.info(f"Backing storage usage: {final_backing_usage:.2f}% ({_format_bytes(backing_free)} free of {_format_bytes(backing_total)} total)")
    
    return {
        'files_moved': files_moved_count,
        'total_bytes': total_bytes,
        'elapsed_time': elapsed_time,
        'final_cache_usage': final_cache_usage,
        'cache_free': cache_free,
        'cache_total': cache_total,
        'final_backing_usage': final_backing_usage,
        'backing_free': backing_free,
        'backing_total': backing_total
    }

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
        commit_hash = get_current_commit_hash()
        notify = NotificationHandler(config, commit_hash)
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

    try:
        if current_usage > config['Settings']['THRESHOLD_PERCENTAGE'] or (config['Settings']['THRESHOLD_PERCENTAGE'] == 0 and config['Settings']['TARGET_PERCENTAGE'] == 0):
            logging.info(f"Cache usage is {current_usage:.2f}%, {'exceeding threshold' if current_usage > config['Settings']['THRESHOLD_PERCENTAGE'] else 'in empty cache mode'}. Starting file move...")
            files_to_move = gather_files_to_move(config)
            
            if args.dry_run:
                stats = move_files_concurrently(files_to_move, config, dry_run=True, stop_event=stop_event)
                empty_dirs_count = remove_empty_dirs(config['Paths']['CACHE_PATH'], 
                                                  config['Settings']['EXCLUDED_DIRS'], 
                                                  dry_run=True)
                
                logging.info("Dry run summary:")
                logging.info(f"- Would move {stats['files_moved']} files")
                logging.info(f"- Would remove {empty_dirs_count} empty directories")
            else:
                stats = move_files_concurrently(files_to_move, config, dry_run=False, stop_event=stop_event)
                empty_dirs_count = 0
                
                if stats['files_moved'] > 0:
                    empty_dirs_count = remove_empty_dirs(config['Paths']['CACHE_PATH'], 
                                                      config['Settings']['EXCLUDED_DIRS'])
                    
                    notify.notify_completion(
                        files_moved=stats['files_moved'],
                        total_bytes=stats['total_bytes'],
                        elapsed_time=stats['elapsed_time'],
                        final_usage=stats['final_cache_usage'],
                        cache_free=stats['cache_free'],
                        cache_total=stats['cache_total'],
                        backing_usage=stats['final_backing_usage'],
                        backing_free=stats['backing_free'],
                        backing_total=stats['backing_total']
                    )
                    logging.info(f"Moved {stats['files_moved']} files in {stats['elapsed_time']:.1f} seconds")
                    logging.info(f"Average speed: {(stats['total_bytes'] / (1024**2)) / stats['elapsed_time']:.1f} MB/s")
                    logging.info(f"Removed {empty_dirs_count} empty directories")
                else:
                    cache_total, cache_used, cache_free = shutil.disk_usage(config['Paths']['CACHE_PATH'])
                    final_cache_usage = (cache_used / cache_total) * 100
                    
                    backing_total, backing_used, backing_free = shutil.disk_usage(config['Paths']['BACKING_PATH'])
                    final_backing_usage = (backing_used / backing_total) * 100

                    notify.notify_threshold_not_met(
                        current_usage=current_usage,
                        threshold=config['Settings']['THRESHOLD_PERCENTAGE'],
                        cache_free=cache_free,
                        cache_total=cache_total,
                        backing_free=backing_free,
                        backing_total=backing_total
                    )
                    logging.info("No files were moved. Skipping directory cleanup.")
        else:
            logging.info(f"Cache usage is {current_usage:.2f}%, below threshold ({config['Settings']['THRESHOLD_PERCENTAGE']}%). No action required.")
            
            # Get disk stats for the notification
            cache_total, cache_used, cache_free = shutil.disk_usage(config['Paths']['CACHE_PATH'])
            backing_total, backing_used, backing_free = shutil.disk_usage(config['Paths']['BACKING_PATH'])
            
            notify.notify_threshold_not_met(
                current_usage=current_usage,
                threshold=config['Settings']['THRESHOLD_PERCENTAGE'],
                cache_free=cache_free,
                cache_total=cache_total,
                backing_free=backing_free,
                backing_total=backing_total
            )

    except Exception as e:
        notify.notify_error(str(e))
        logging.error(f"Error during execution: {e}")
        raise

    if stop_event.is_set():
        logging.info("Script execution interrupted. Some operations may not have completed.")

if __name__ == "__main__":
    main()