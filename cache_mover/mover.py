import os
import shutil
import logging
import stat
import secrets
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from time import time

from .filesystem import get_fs_usage, get_fs_free_space, _format_bytes
from .hardlink_manager import create_hardlink_safe

def generate_temp_filename(original_path):
    dir_name = os.path.dirname(original_path)
    base_name = os.path.basename(original_path)
    suffix = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(6))
    temp_name = f".{base_name}.{suffix}"
    return os.path.join(dir_name, temp_name)

def makedirs_preserve_stats(src_dir, dest_dir): # dir tree perm fix attempt v1.3.3
    if not src_dir or not dest_dir:
        logging.warning("Invalid directory paths provided")
        return
        
    if os.path.exists(dest_dir):
        return
        
    dirs_to_create = []
    current_dir = dest_dir
    while not os.path.exists(current_dir):
        dirs_to_create.append(current_dir)
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir: # root dir reached
            break
        current_dir = parent_dir
    
    for new_dir in reversed(dirs_to_create):
        try:
            os.makedirs(new_dir, exist_ok=True)
            
            try:
                rel_path = os.path.relpath(new_dir, os.path.dirname(dest_dir))
                src_path = os.path.normpath(os.path.join(os.path.dirname(src_dir), rel_path))
                
                if os.path.exists(src_path):
                    src_stat = os.stat(src_path)
                    os.chown(new_dir, src_stat.st_uid, src_stat.st_gid)
                    os.chmod(new_dir, src_stat.st_mode)
                    logging.debug(f"Set permissions on {new_dir} from {src_path}")
                else:
                    logging.debug(f"Source directory does not exist: {src_path}")
            except (OSError, ValueError) as e:
                logging.warning(f"Failed to calculate or access source path for {new_dir}: {e}")
        except OSError as e:
            logging.error(f"Failed to create or set permissions for {new_dir}: {e}")
            raise

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
            makedirs_preserve_stats(os.path.dirname(src), dest_dir)

        if not dry_run and get_fs_free_space(backing_path) < file_size:
            logging.error(f"Not enough space in backing storage for {src}")
            return False, 0, 0

        if not dry_run:
            temp_dest = generate_temp_filename(dest)
            logging.debug(f"Using temp file: {temp_dest}")
            
            try:
                shutil.copy2(src, temp_dest)
                
                temp_stat_after_copy = os.stat(temp_dest)
                logging.debug(f"Temp file {temp_dest} permissions after copy2: mode={oct(stat.S_IMODE(temp_stat_after_copy.st_mode))}, uid={temp_stat_after_copy.st_uid}, gid={temp_stat_after_copy.st_gid}")
                
                try:
                    os.chown(temp_dest, src_stat.st_uid, src_stat.st_gid)
                    os.chmod(temp_dest, stat.S_IMODE(src_stat.st_mode))
                    
                    temp_stat_final = os.stat(temp_dest)
                    logging.debug(f"Temp file {temp_dest} final permissions: mode={oct(stat.S_IMODE(temp_stat_final.st_mode))}, uid={temp_stat_final.st_uid}, gid={temp_stat_final.st_gid}")
                except OSError as e:
                    logging.warning(f"Failed to set ownership/permissions for {temp_dest}: {e}")
                
                if os.path.getsize(temp_dest) != file_size:
                    logging.error(f"Size mismatch after copying {src}")
                    try:
                        os.remove(temp_dest)
                    except OSError:
                        pass
                    return False, 0, 0
                
                os.rename(temp_dest, dest)
                logging.debug(f"Renamed temp file to final destination: {dest}")
                
                try:
                    os.remove(src)
                except OSError as e:
                    logging.error(f"Failed to remove source file {src}: {e}")
                    try:
                        os.remove(dest)
                    except OSError:
                        pass
                    return False, 0, 0
                    
            except OSError as e:
                logging.error(f"Error during temp file operation for {src}: {e}")
                try:
                    if os.path.exists(temp_dest):
                        os.remove(temp_dest)
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
        first_dest_final = os.path.join(dest_base, os.path.relpath(hardlink_group[0], cache_path))
        first_temp_dest = None

        for src in hardlink_group:
            rel_path = os.path.relpath(src, cache_path)
            dest = os.path.join(dest_base, rel_path)
            dest_dir = os.path.dirname(dest)

            if not dry_run and not os.path.exists(dest_dir):
                makedirs_preserve_stats(os.path.dirname(src), dest_dir)

            if not dry_run:
                if src == hardlink_group[0]:
                    first_temp_dest = generate_temp_filename(dest)
                    logging.debug(f"Using temp file for hardlink group: {first_temp_dest}")
                    
                    shutil.copy2(src, first_temp_dest)
                    try:
                        os.chown(first_temp_dest, src_stat.st_uid, src_stat.st_gid)
                        os.chmod(first_temp_dest, stat.S_IMODE(src_stat.st_mode))
                    except OSError as e:
                        logging.info(f"Failed to set ownership/permissions for {first_temp_dest}: {e}")
                    
                    if os.path.getsize(first_temp_dest) != file_size:
                        logging.error(f"Size mismatch after copying hardlinked files")
                        try:
                            os.remove(first_temp_dest)
                        except OSError:
                            pass
                        return False, 0, 0
                    
                    os.rename(first_temp_dest, dest)
                    logging.debug(f"Renamed temp file to final destination: {dest}")
                else:
                    try:
                        if not create_hardlink_safe(first_dest_final, dest, backing_path):
                            logging.error(f"Failed to create hardlink for {src}")
                            return False, 0, 0
                    except Exception as e:
                        logging.error(f"Exception creating hardlink for {src}: {e}")
                        return False, 0, 0

        if not dry_run:
            if os.path.getsize(first_dest_final) == file_size:
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
        log_record.dest = first_dest_final
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
            makedirs_preserve_stats(os.path.dirname(src), dest_dir)

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
    avg_speed = total_bytes_moved / elapsed_time if elapsed_time > 0 else 0

    if moved_count > 0:
        # Convert to MB/s for logging
        mb_per_second = avg_speed / (1024 * 1024)
        logging.info(
            f"Moved {moved_count} files ({_format_bytes(total_bytes_moved)}) "
            f"in {elapsed_time:.1f}s ({mb_per_second:.2f} MB/s)"
        )

    return moved_count, total_bytes_moved, elapsed_time, avg_speed
