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

def move_hardlinked_files(hardlink_group, dest_base, config, target_reached_lock, dry_run=False, stop_event=None):
    """
    Move a group of hardlinked files while preserving their hardlinks.
    
    Args:
        hardlink_group (list): List of hardlinked file paths
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
    
    try:
        # Use the first file in the group to get size and check space
        src_first = hardlink_group[0]
        file_size = os.path.getsize(src_first)
        start_time = time()
        
        # Check if we've reached the target
        with target_reached_lock:
            current_usage = get_fs_usage(cache_path)
            if current_usage <= target_percentage:
                return False, 0, 0

        # Check if we have enough space
        if not dry_run and get_fs_free_space(backing_path) < file_size:
            logging.error(f"Not enough space in backing storage for hardlinked files")
            return False, 0, 0

        # Process each file in the hardlink group
        for src in hardlink_group:
            rel_path = os.path.relpath(src, cache_path)
            dest = os.path.join(dest_base, rel_path)
            dest_dir = os.path.dirname(dest)

            # Ensure destination directory exists
            if not dry_run and not os.path.exists(dest_dir):
                os.makedirs(dest_dir, exist_ok=True)

            # Move the file
            if not dry_run:
                # Copy the first file normally
                if src == hardlink_group[0]:
                    shutil.copy2(src, dest)
                else:
                    # Create hardlinks for subsequent files
                    try:
                        first_dest = os.path.join(dest_base, os.path.relpath(hardlink_group[0], cache_path))
                        os.link(first_dest, dest)
                    except OSError as e:
                        logging.error(f"Failed to create hardlink for {src}: {e}")
                        return False, 0, 0

        # Verify and cleanup
        if not dry_run:
            # Verify first file
            first_dest = os.path.join(dest_base, os.path.relpath(hardlink_group[0], cache_path))
            if os.path.getsize(first_dest) == file_size:
                # Remove source files
                for src in hardlink_group:
                    try:
                        os.remove(src)
                    except OSError as e:
                        logging.error(f"Failed to remove source file {src}: {e}")
                        # Try to clean up destination files
                        for dest_file in [os.path.join(dest_base, os.path.relpath(f, cache_path)) for f in hardlink_group]:
                            try:
                                os.remove(dest_file)
                            except OSError:
                                pass
                        return False, 0, 0
            else:
                logging.error(f"Size mismatch after copying hardlinked files")
                # Clean up destination files
                for dest_file in [os.path.join(dest_base, os.path.relpath(f, cache_path)) for f in hardlink_group]:
                    try:
                        os.remove(dest_file)
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
            msg=f"{'Would move' if dry_run else 'Moved'} hardlinked group ({len(hardlink_group)} files) {_format_bytes(file_size)}",
            args=(),
            exc_info=None
        )
        log_record.src = hardlink_group[0]
        log_record.dest = os.path.join(dest_base, os.path.relpath(hardlink_group[0], cache_path))
        log_record.file_move = True
        logging.getLogger().handle(log_record)

        return True, file_size, time_taken
    except (OSError, IOError) as e:
        logging.error(f"Error moving hardlinked files: {e}")
        return False, 0, 0

def move_symlink(src, dest_base, config, target_reached_lock, dry_run=False, stop_event=None):
    """
    Move a symbolic link by recreating it at the destination.
    Handles cases where the symlink target is also being moved from cache to backing storage.
    
    Args:
        src (str): Source symlink path
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
        start_time = time()
        
        # Check if we've reached the target
        with target_reached_lock:
            current_usage = get_fs_usage(cache_path)
            if current_usage <= target_percentage:
                return False, 0, 0

        # Get the target of the symlink
        target = os.readlink(src)
        
        # Convert target to absolute path if it's relative
        if not os.path.isabs(target):
            target = os.path.normpath(os.path.join(os.path.dirname(src), target))
        
        # Check if the target is within the cache path
        try:
            rel_target = os.path.relpath(target, cache_path)
            # If we get here without ValueError, the target is within cache_path
            if not rel_target.startswith('..'):
                # Target is in cache, so it will be moved to backing storage
                # Update target to point to new location
                target = os.path.join(backing_path, rel_target)
                logging.debug(f"Symlink target {rel_target} is in cache, updating to point to backing storage: {target}")
        except ValueError:
            # Target is outside cache_path, keep original target
            pass

        # Ensure destination directory exists
        if not dry_run and not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)

        # Create the symlink at the destination
        if not dry_run:
            try:
                # Remove destination if it exists
                if os.path.lexists(dest):
                    os.remove(dest)
                
                # Create the new symlink
                os.symlink(target, dest)
                
                # Remove the source symlink
                os.remove(src)
            except OSError as e:
                logging.error(f"Failed to move symlink {src}: {e}")
                # Try to clean up if something went wrong
                if os.path.exists(dest):
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
            msg=f"{'Would move' if dry_run else 'Moved'} symlink -> {target}",
            args=(),
            exc_info=None
        )
        log_record.src = src
        log_record.dest = dest
        log_record.file_move = True
        logging.getLogger().handle(log_record)

        # Return 0 for bytes_moved since symlinks don't count towards storage
        return True, 0, time_taken
    except (OSError, IOError) as e:
        logging.error(f"Error moving symlink {src}: {e}")
        return False, 0, 0

def move_files_concurrently(files_to_move, config, dry_run=False, stop_event=None):
    """
    Move files concurrently using a thread pool.
    
    Args:
        files_to_move (tuple): Tuple of (regular_files, hardlink_groups, symlinks)
        config (dict): Configuration dictionary
        dry_run (bool): If True, only simulate operations
        stop_event (Event): Event to signal stopping
    
    Returns:
        tuple: (moved_count, total_bytes_moved, elapsed_time, avg_speed)
    """
    regular_files, hardlink_groups, symlinks = files_to_move
    if not regular_files and not hardlink_groups and not symlinks:
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
        # Submit regular file moves
        future_to_file = {
            executor.submit(
                move_file, 
                src, 
                backing_path, 
                config,
                target_reached_lock,
                dry_run,
                stop_event
            ): src for src in regular_files
        }
        
        # Submit hardlinked file group moves
        for inode, group in hardlink_groups.items():
            future_to_file[
                executor.submit(
                    move_hardlinked_files,
                    group,
                    backing_path,
                    config,
                    target_reached_lock,
                    dry_run,
                    stop_event
                )
            ] = f"hardlink_group_{inode}"

        # Submit symlink moves
        for src, target in symlinks.items():
            future_to_file[
                executor.submit(
                    move_symlink,
                    src,
                    backing_path,
                    config,
                    target_reached_lock,
                    dry_run,
                    stop_event
                )
            ] = f"symlink_{src}"

        for future in as_completed(future_to_file):
            if stop_event and stop_event.is_set():
                break

            src = future_to_file[future]
            try:
                success, bytes_moved, time_taken = future.result()
                if success:
                    # For hardlink groups, count each file in the group
                    if isinstance(src, str) and src.startswith("hardlink_group_"):
                        inode = int(src.split("_")[-1])
                        moved_count += len(hardlink_groups[inode])
                    # For symlinks, just count as one file
                    elif isinstance(src, str) and src.startswith("symlink_"):
                        moved_count += 1
                    else:
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