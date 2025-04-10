import os
import shutil
import logging
import stat
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from time import time

from .filesystem import get_fs_usage, get_fs_free_space, _format_bytes
from .hardlink_manager import create_hardlink_safe

def move_file(src, dest_base, config, target_reached_lock, dry_run=False, stop_event=None):
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
        
        with target_reached_lock:
            current_usage = get_fs_usage(cache_path)
            if current_usage <= target_percentage:
                return False, 0, 0

        src_stat = os.stat(src)
        logging.debug(f"Source file {src} permissions: mode={oct(stat.S_IMODE(src_stat.st_mode))}, uid={src_stat.st_uid}, gid={src_stat.st_gid}")

        if not dry_run and not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
            src_dir = os.path.dirname(src)
            try:
                src_dir_stat = os.stat(src_dir)
                os.chown(dest_dir, src_dir_stat.st_uid, src_dir_stat.st_gid)
                os.chmod(dest_dir, src_dir_stat.st_mode)
            except OSError as e:
                logging.info(f"Failed to set directory ownership/permissions for {dest_dir}: {e}")

        if not dry_run and get_fs_free_space(backing_path) < file_size:
            logging.error(f"Not enough space in backing storage for {src}")
            return False, 0, 0

        if not dry_run:
            shutil.copy2(src, dest)
            
            dest_stat_after_copy = os.stat(dest)
            logging.debug(f"Destination file {dest} permissions after copy2: mode={oct(stat.S_IMODE(dest_stat_after_copy.st_mode))}, uid={dest_stat_after_copy.st_uid}, gid={dest_stat_after_copy.st_gid}")
            
            try:
                os.chown(dest, src_stat.st_uid, src_stat.st_gid)
                os.chmod(dest, stat.S_IMODE(src_stat.st_mode))
                
                dest_stat_final = os.stat(dest)
                logging.debug(f"Destination file {dest} final permissions: mode={oct(stat.S_IMODE(dest_stat_final.st_mode))}, uid={dest_stat_final.st_uid}, gid={dest_stat_final.st_gid}")
            except OSError as e:
                logging.warning(f"Failed to set ownership/permissions for {dest}: {e}")
            
            if os.path.getsize(dest) == file_size:
                try:
                    os.remove(src)
                except OSError as e:
                    logging.error(f"Failed to remove source file {src}: {e}")
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
    if stop_event and stop_event.is_set():
        return False, 0, 0

    cache_path = config['Paths']['CACHE_PATH']
    backing_path = config['Paths']['BACKING_PATH']
    target_percentage = config['Settings']['TARGET_PERCENTAGE']
    
    try:
        src_first = hardlink_group[0]
        file_size = os.path.getsize(src_first)
        start_time = time()
        
        with target_reached_lock:
            current_usage = get_fs_usage(cache_path)
            if current_usage <= target_percentage:
                return False, 0, 0

        if not dry_run and get_fs_free_space(backing_path) < file_size:
            logging.error(f"Not enough space in backing storage for hardlinked files")
            return False, 0, 0

        src_stat = os.stat(src_first)

        for src in hardlink_group:
            rel_path = os.path.relpath(src, cache_path)
            dest = os.path.join(dest_base, rel_path)
            dest_dir = os.path.dirname(dest)

            if not dry_run and not os.path.exists(dest_dir):
                os.makedirs(dest_dir, exist_ok=True)
                src_dir = os.path.dirname(src)
                try:
                    src_dir_stat = os.stat(src_dir)
                    os.chown(dest_dir, src_dir_stat.st_uid, src_dir_stat.st_gid)
                    os.chmod(dest_dir, src_dir_stat.st_mode)
                except OSError as e:
                    logging.warning(f"Failed to set directory ownership/permissions for {dest_dir}: {e}")

            if not dry_run:
                if src == hardlink_group[0]:
                    shutil.copy2(src, dest)
                    try:
                        os.chown(dest, src_stat.st_uid, src_stat.st_gid)
                        os.chmod(dest, stat.S_IMODE(src_stat.st_mode))
                    except OSError as e:
                        logging.warning(f"Failed to set ownership/permissions for {dest}: {e}")
                else:
                    try:
                        first_dest = os.path.join(dest_base, os.path.relpath(hardlink_group[0], cache_path))
                        if not create_hardlink_safe(first_dest, dest, backing_path):
                            logging.error(f"Failed to create hardlink for {src}")
                            return False, 0, 0
                    except Exception as e:
                        logging.error(f"Exception creating hardlink for {src}: {e}")
                        return False, 0, 0

        if not dry_run:
            first_dest = os.path.join(dest_base, os.path.relpath(hardlink_group[0], cache_path))
            if os.path.getsize(first_dest) == file_size:
                for src in hardlink_group:
                    try:
                        os.remove(src)
                    except OSError as e:
                        logging.error(f"Failed to remove source file {src}: {e}")
                        for dest_file in [os.path.join(dest_base, os.path.relpath(f, cache_path)) for f in hardlink_group]:
                            try:
                                os.remove(dest_file)
                            except OSError:
                                pass
                        return False, 0, 0
            else:
                logging.error(f"Size mismatch after copying hardlinked files")
                for dest_file in [os.path.join(dest_base, os.path.relpath(f, cache_path)) for f in hardlink_group]:
                    try:
                        os.remove(dest_file)
                    except OSError:
                        pass
                return False, 0, 0

        end_time = time()
        time_taken = end_time - start_time

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
        
        with target_reached_lock:
            current_usage = get_fs_usage(cache_path)
            if current_usage <= target_percentage:
                return False, 0, 0

        target = os.readlink(src)
        
        if not os.path.isabs(target):
            target = os.path.normpath(os.path.join(os.path.dirname(src), target))
        
        try:
            rel_target = os.path.relpath(target, cache_path)
            if not rel_target.startswith('..'):
                target = os.path.join(backing_path, rel_target)
                logging.debug(f"Symlink target {rel_target} is in cache, updating to point to backing storage: {target}")
        except ValueError:
            pass

        if not dry_run and not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
            src_dir = os.path.dirname(src)
            try:
                src_dir_stat = os.stat(src_dir)
                os.chown(dest_dir, src_dir_stat.st_uid, src_dir_stat.st_gid)
                os.chmod(dest_dir, src_dir_stat.st_mode)
            except OSError as e:
                logging.warning(f"Failed to set directory ownership/permissions for {dest_dir}: {e}")

        if not dry_run:
            try:
                if os.path.lexists(dest):
                    os.remove(dest)
                
                os.symlink(target, dest)
                
                os.remove(src)
            except OSError as e:
                logging.error(f"Failed to move symlink {src}: {e}")
                if os.path.exists(dest):
                    try:
                        os.remove(dest)
                    except OSError:
                        pass
                return False, 0, 0

        end_time = time()
        time_taken = end_time - start_time

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

        return True, 0, time_taken
    except (OSError, IOError) as e:
        logging.error(f"Error moving symlink {src}: {e}")
        return False, 0, 0

def move_files_concurrently(files_to_move, config, dry_run=False, stop_event=None):
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
                    if isinstance(src, str) and src.startswith("hardlink_group_"):
                        inode = int(src.split("_")[-1])
                        moved_count += len(hardlink_groups[inode])
                    elif isinstance(src, str) and src.startswith("symlink_"):
                        moved_count += 1
                    else:
                        moved_count += 1
                    total_bytes_moved += bytes_moved
                    total_time += time_taken
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