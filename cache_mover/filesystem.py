import os
import shutil
import stat
import logging
import psutil
from collections import defaultdict

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

def get_file_inode(path):
    """
    Get the inode number of a file.
    
    Args:
        path (str): Path to the file
    
    Returns:
        int: Inode number of the file
    """
    try:
        return os.stat(path).st_ino
    except (OSError, IOError) as e:
        logging.warning(f"Error getting inode for {path}: {e}")
        return None

def get_hardlink_groups(files):
    """
    Group files by their inode numbers to identify hardlinks.
    
    Args:
        files (list): List of file paths
    
    Returns:
        dict: Dictionary mapping inode numbers to lists of file paths
    """
    hardlink_groups = defaultdict(list)
    
    for file_path in files:
        try:
            inode = get_file_inode(file_path)
            if inode is not None:
                nlink = os.stat(file_path).st_nlink
                # Only track files that have more than one link
                if nlink > 1:
                    hardlink_groups[inode].append(file_path)
        except (OSError, IOError) as e:
            logging.warning(f"Error processing hardlink for {file_path}: {e}")
            continue
    
    # Filter out single-file groups (not hardlinked)
    return {k: v for k, v in hardlink_groups.items() if len(v) > 1}

def is_symlink(path):
    """
    Check if a path is a symbolic link.
    
    Args:
        path (str): Path to check
    
    Returns:
        tuple: (is_symlink, target_path) if symlink, (False, None) otherwise
    """
    try:
        if os.path.islink(path):
            return True, os.readlink(path)
        return False, None
    except (OSError, IOError) as e:
        logging.warning(f"Error checking symlink for {path}: {e}")
        return False, None

def gather_files_to_move(config):
    """
    Gather list of files to move from cache to backing storage.
    Groups hardlinked files together and identifies symlinks.
    
    Args:
        config (dict): Configuration dictionary
    
    Returns:
        tuple: (list of regular files to move, dict of hardlink groups, dict of symlinks)
    """
    cache_path = config['Paths']['CACHE_PATH']
    excluded_dirs = config['Settings']['EXCLUDED_DIRS']
    files_to_move = []
    symlinks = {}

    for root, _, files in os.walk(cache_path):
        if is_excluded(root, excluded_dirs):
            continue
            
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Check if it's a symlink first
                is_link, target = is_symlink(file_path)
                if is_link:
                    symlinks[file_path] = target
                    continue

                # Skip files that are currently being written
                if os.path.getsize(file_path) == 0:
                    continue
                files_to_move.append(file_path)
            except (OSError, IOError) as e:
                logging.warning(f"Error accessing file {file_path}: {e}")
                continue

    # Separate hardlinked files from regular files
    hardlink_groups = get_hardlink_groups(files_to_move)
    hardlinked_files = set(f for group in hardlink_groups.values() for f in group)
    regular_files = [f for f in files_to_move if f not in hardlinked_files]

    return regular_files, hardlink_groups, symlinks

def remove_empty_dirs(path, excluded_dirs, dry_run=False):
    """
    Recursively remove empty directories while preserving parent structure.
    Only removes directories that become empty after file moves.
    
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
                # Skip if directory is excluded
                if is_excluded(dir_path, excluded_dirs):
                    continue
                    
                # Only remove if directory is empty
                if not os.listdir(dir_path):
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