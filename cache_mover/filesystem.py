import os
import shutil
import stat
import logging
import psutil
from collections import defaultdict

def get_fs_usage(path):
    usage = shutil.disk_usage(path)
    return (usage.used / usage.total) * 100

def get_fs_free_space(path):
    return shutil.disk_usage(path).free

def _format_bytes(bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.2f}{unit}"
        bytes /= 1024
    return f"{bytes:.2f}PB"

def is_excluded(path, excluded_dirs):
    normalized_path = os.path.normpath(path)
    
    for excluded in excluded_dirs:
        excluded_normalized = os.path.normpath(excluded)
        
        # subdirectory path pattern (e.g., 'media/downloads')
        if os.sep in excluded_normalized:
            path_forward = normalized_path.replace(os.sep, '/')
            excluded_forward = excluded_normalized.replace(os.sep, '/')
            
            if f'/{excluded_forward}/' in f'/{path_forward}/':
                return True
        # single directory name (e.g., 'downloads')
        else:
            path_parts = [part for part in normalized_path.split(os.sep) if part]
            if excluded_normalized in path_parts:
                return True
    
    return False

def get_file_inode(path):
    try:
        return os.stat(path).st_ino
    except (OSError, IOError) as e:
        logging.warning(f"Error getting inode for {path}: {e}")
        return None

def get_hardlink_groups(files):
    hardlink_groups = defaultdict(list)
    
    for file_path in files:
        try:
            inode = get_file_inode(file_path)
            if inode is not None:
                nlink = os.stat(file_path).st_nlink
                if nlink > 1:
                    hardlink_groups[inode].append(file_path)
        except (OSError, IOError) as e:
            logging.warning(f"Error processing hardlink for {file_path}: {e}")
            continue
    
    return {k: v for k, v in hardlink_groups.items() if len(v) > 1}

def is_symlink(path):
    try:
        if os.path.islink(path):
            return True, os.readlink(path)
        return False, None
    except (OSError, IOError) as e:
        logging.warning(f"Error checking symlink for {path}: {e}")
        return False, None

def gather_files_to_move(config):
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
                is_link, target = is_symlink(file_path)
                if is_link:
                    symlinks[file_path] = target
                    continue

                if os.path.getsize(file_path) == 0:
                    continue
                files_to_move.append(file_path)
            except (OSError, IOError) as e:
                logging.warning(f"Error accessing file {file_path}: {e}")
                continue

    hardlink_groups = get_hardlink_groups(files_to_move)
    hardlinked_files = set(f for group in hardlink_groups.values() for f in group)
    regular_files = [f for f in files_to_move if f not in hardlinked_files]

    return regular_files, hardlink_groups, symlinks

def remove_empty_dirs(path, excluded_dirs, dry_run=False):
    removed = 0
    for root, dirs, files in os.walk(path, topdown=False):
        if is_excluded(root, excluded_dirs):
            continue
            
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                if is_excluded(dir_path, excluded_dirs):
                    continue
                    
                if not os.listdir(dir_path):
                    if not dry_run:
                        os.rmdir(dir_path)
                    logging.info(f"{'Would remove' if dry_run else 'Removed'} empty directory: {dir_path}")
                    removed += 1
            except OSError as e:
                logging.warning(f"Error removing directory {dir_path}: {e}")
                
    return removed

def is_script_running(instance_id=None):
    current_process = psutil.Process()
    current_script = os.path.abspath(__file__)
    script_name = os.path.basename(current_script)
    
    # set instance_id env var for process
    if instance_id:
        os.environ['CACHE_MOVER_INSTANCE_ID'] = instance_id
    
    if os.environ.get('DOCKER_CONTAINER'):
        container_processes = [p for p in psutil.process_iter(['pid', 'name', 'cmdline', 'environ']) 
                             if p.pid != current_process.pid]
        running_instances = []
        
        for process in container_processes:
            try:
                if process.name() == 'python' or process.name() == 'python3':
                    cmdline = process.cmdline()
                    if len(cmdline) >= 2 and script_name in cmdline[-1]:
                        if not is_child_process(current_process, process):
                            # block same instance_id
                            if instance_id:
                                try:
                                    proc_env = process.environ()
                                    proc_instance_id = proc_env.get('CACHE_MOVER_INSTANCE_ID')
                                    if proc_instance_id == instance_id:
                                        running_instances.append(f"{' '.join(cmdline)} [instance: {instance_id}]")
                                except (psutil.AccessDenied, psutil.NoSuchProcess):
                                    pass
                            else:
                                running_instances.append(' '.join(cmdline))
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
                
        return bool(running_instances), running_instances
    
    for process in psutil.process_iter(['pid', 'name', 'cmdline', 'environ']):
        if process.pid != current_process.pid:
            try:
                if process.name() == 'python' or process.name() == 'python3':
                    cmdline = process.cmdline()
                    if len(cmdline) >= 2 and script_name in cmdline[-1]:
                        if not is_child_process(current_process, process):
                            # block same instance_id
                            if instance_id:
                                try:
                                    proc_env = process.environ()
                                    proc_instance_id = proc_env.get('CACHE_MOVER_INSTANCE_ID')
                                    if proc_instance_id == instance_id:
                                        return True, [f"{' '.join(cmdline)} [instance: {instance_id}]"]
                                except (psutil.AccessDenied, psutil.NoSuchProcess):
                                    pass
                            else:
                                return True, [' '.join(cmdline)]
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    return False, []

def is_child_process(parent, child):
    try:
        return child.ppid() == parent.pid
    except psutil.NoSuchProcess:
        return False 