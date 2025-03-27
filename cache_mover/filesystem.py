import os
import shutil
import stat
import logging
import psutil

def get_fs_usage(path):
    """Get filesystem usage percentage for the given path."""
    usage = shutil.disk_usage(path)
    return (usage.used / usage.total) * 100

def get_fs_free_space(path):
    """Get free space in bytes for the given path."""
    return shutil.disk_usage(path).free

def _format_bytes(bytes: int) -> str:
    """Format bytes into human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.2f}{unit}"
        bytes /= 1024
    return f"{bytes:.2f}PB"

def is_excluded(path, excluded_dirs):
    """
    Check if a path should be excluded based on excluded directories.
    
    Args:
        path (str): Path to check
        excluded_dirs (list): List of directory names to exclude
    
    Returns:
        bool: True if path should be excluded, False otherwise
    """
    path_parts = path.split(os.sep)
    return any(excluded in path_parts for excluded in excluded_dirs)

def gather_files_to_move(config):
    """
    Gather list of files to move from cache to backing storage.
    
    Args:
        config (dict): Configuration dictionary
    
    Returns:
        list: List of file paths to move
    """
    cache_path = config['Paths']['CACHE_PATH']
    excluded_dirs = config['Settings']['EXCLUDED_DIRS']
    files_to_move = []

    for root, _, files in os.walk(cache_path):
        if is_excluded(root, excluded_dirs):
            continue
            
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Skip files that are currently being written
                if os.path.getsize(file_path) == 0:
                    continue
                files_to_move.append(file_path)
            except (OSError, IOError) as e:
                logging.warning(f"Error accessing file {file_path}: {e}")
                continue

    return files_to_move

def remove_empty_dirs(path, excluded_dirs, dry_run=False):
    """
    Recursively remove empty directories.
    
    Args:
        path (str): Starting path
        excluded_dirs (list): List of directory names to exclude
        dry_run (bool): If True, only log actions without performing them
    
    Returns:
        int: Number of directories removed
    """
    removed = 0
    for root, dirs, files in os.walk(path, topdown=False):
        if is_excluded(root, excluded_dirs):
            continue
            
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                if not os.listdir(dir_path):  # Directory is empty
                    if not dry_run:
                        os.rmdir(dir_path)
                    logging.info(f"{'Would remove' if dry_run else 'Removed'} empty directory: {dir_path}")
                    removed += 1
            except OSError as e:
                logging.warning(f"Error removing directory {dir_path}: {e}")
                
    return removed

def is_script_running():
    """
    Check if another instance of the script is already running.
    
    Returns:
        tuple: (bool, list) - (is_running, list of running instances)
    """
    current_process = psutil.Process()
    current_script = os.path.abspath(__file__)
    script_name = os.path.basename(current_script)
    
    # Docker check for process inside container
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
    
    # Non-docker environment
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
    """Check if one process is a child of another."""
    try:
        return child.ppid() == parent.pid
    except psutil.NoSuchProcess:
        return False 