"""
Core file moving functionality.
"""

import os
import shutil
import logging
import stat
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from time import time

from .filesystem import get_fs_usage, get_fs_free_space, _format_bytes

def move_file(src, dest_base, config, target_reached_lock, dry_run=False, stop_event=None):
    """
    Move a single file from cache to backing storage.
    
    Args:
        src (str): Source file path
        dest_base (str): Base destination directory
        config (dict): Configuration dictionary
        target_reached_lock (Lock): Lock for thread synchronization
        dry_run (bool): If True, only simulate operations
        stop_event (Event): Event to signal stopping
    
    Returns:
        tuple: (success, bytes_moved, time_taken)
    """
    if stop_event and stop_event.is_set():
        return False, 0, 0

    cache_path = config['Paths']['CACHE_PATH']
    backing_path = config['Paths']['BACKING_PATH']
    target_percentage = config['Settings']['TARGET_PERCENTAGE']
    
    rel_path = os.path.relpath(src, cache_path)
    dest = os.path.join(dest_base, rel_path)
    dest_dir = os.path.dirname(dest)

    try:
        file_size = os.path.getsize(src)
        start_time = time()
        
        # Check if we've reached the target
        with target_reached_lock:
            current_usage = get_fs_usage(cache_path)
            if current_usage <= target_percentage:
                return False, 0, 0

        # Ensure destination directory exists
        if not dry_run and not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)

        # Check if we have enough space
        if not dry_run and get_fs_free_space(backing_path) < file_size:
            logging.error(f"Not enough space in backing storage for {src}")
            return False, 0, 0

        # Move the file
        if not dry_run:
            # Copy file with original permissions
            shutil.copy2(src, dest)
            
            # Verify the copy
            if os.path.getsize(dest) == file_size:
                try:
                    # Remove source file
                    os.remove(src)
                except OSError as e:
                    logging.error(f"Failed to remove source file {src}: {e}")
                    # Try to remove destination file if source removal failed
                    try:
                        os.remove(dest)
                    except OSError:
                        pass
                    return False, 0, 0
            else:
                logging.error(f"Size mismatch after copying {src}")
                try:
                    os.remove(dest)
                except OSError:
                    pass
                return False, 0, 0

        end_time = time()
        time_taken = end_time - start_time

        # Log the move operation
        log_record = logging.LogRecord(
            name='cache_mover',
            level=logging.INFO,
            pathname=__file__,
            lineno=0,
            msg=f"{'Would move' if dry_run else 'Moved'} {_format_bytes(file_size)}",
            args=(),
            exc_info=None
        )
        log_record.src = src
        log_record.dest = dest
        log_record.file_move = True
        logging.getLogger().handle(log_record)

        return True, file_size, time_taken
    except (OSError, IOError) as e:
        logging.error(f"Error moving file {src}: {e}")
        return False, 0, 0

def move_files_concurrently(files_to_move, config, dry_run=False, stop_event=None):
    """
    Move files concurrently using a thread pool.
    
    Args:
        files_to_move (list): List of files to move
        config (dict): Configuration dictionary
        dry_run (bool): If True, only simulate operations
        stop_event (Event): Event to signal stopping
    
    Returns:
        tuple: (moved_count, total_bytes_moved, elapsed_time, avg_speed)
    """
    if not files_to_move:
        return 0, 0, 0, 0

    cache_path = config['Paths']['CACHE_PATH']
    backing_path = config['Paths']['BACKING_PATH']
    max_workers = config['Settings']['MAX_WORKERS']
    target_percentage = config['Settings']['TARGET_PERCENTAGE']
    
    target_reached_lock = Lock()
    moved_count = 0
    total_bytes_moved = 0
    total_time = 0
    start_time = time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(
                move_file, 
                src, 
                backing_path, 
                config,
                target_reached_lock,
                dry_run,
                stop_event
            ): src for src in files_to_move
        }

        for future in as_completed(future_to_file):
            if stop_event and stop_event.is_set():
                break

            src = future_to_file[future]
            try:
                success, bytes_moved, time_taken = future.result()
                if success:
                    moved_count += 1
                    total_bytes_moved += bytes_moved
                    total_time += time_taken

                    # Check if we've reached the target
                    current_usage = get_fs_usage(cache_path)
                    if current_usage <= target_percentage:
                        if stop_event:
                            stop_event.set()
                        logging.info(f"Target usage of {target_percentage}% reached. Stopping.")
                        break

            except Exception as e:
                logging.error(f"Error processing {src}: {e}")

    elapsed_time = time() - start_time
    avg_speed = total_bytes_moved / (1024 * 1024 * elapsed_time) if elapsed_time > 0 else 0

    if moved_count > 0:
        logging.info(
            f"Moved {moved_count} files ({_format_bytes(total_bytes_moved)}) "
            f"in {elapsed_time:.1f}s ({avg_speed:.2f}MB/s)"
        )

    return moved_count, total_bytes_moved, elapsed_time, avg_speed 