import os
import re
import logging

from .filesystem import _format_bytes

TEMP_FILE_PATTERN = re.compile(r'^\..+\.[a-zA-Z0-9]{6}$')
DELETED_FILE_PATTERN = re.compile(r'^.+\.deleted\.[a-fA-F0-9]{6}$')

def is_temp_file(filename):
    return TEMP_FILE_PATTERN.match(filename) is not None

def is_deleted_marker(filename):
    return DELETED_FILE_PATTERN.match(filename) is not None

def cleanup_orphaned_temp_files(backing_path, excluded_dirs, dry_run=False):
    cleaned_count = 0
    cleaned_size = 0
    restored_count = 0
    restored_size = 0
    
    logging.info("Scanning for orphaned temp files and deleted markers from previous runs")
    logging.debug(f"Excluded directories: {excluded_dirs}")
    
    temp_files = {}
    deleted_markers = {}
    
    for root, dirs, files in os.walk(backing_path):
        original_dirs = dirs.copy()
        dirs[:] = [d for d in dirs if d not in excluded_dirs]
        if len(dirs) != len(original_dirs):
            filtered = set(original_dirs) - set(dirs)
            logging.debug(f"Filtered out directories at {root}: {filtered}")
        
        for filename in files:
            filepath = os.path.join(root, filename)
            
            if is_temp_file(filename):
                temp_files[filepath] = filename
            elif is_deleted_marker(filename):
                deleted_markers[filepath] = filename
    
    for marker_path, marker_name in deleted_markers.items():
        try:
            original_name = marker_name.rsplit('.deleted.', 1)[0]
            original_path = os.path.join(os.path.dirname(marker_path), original_name)
            
            if os.path.exists(original_path):
                file_size = os.path.getsize(marker_path)
                if not dry_run:
                    os.remove(marker_path)
                logging.info(f"{'Would clean' if dry_run else 'Cleaned'} completed move marker: {marker_path}")
                cleaned_count += 1
                cleaned_size += file_size
            else:
                file_size = os.path.getsize(marker_path)
                if not dry_run:
                    os.rename(marker_path, original_path)
                logging.warning(f"{'Would restore' if dry_run else 'Restored'} interrupted move: {marker_path} -> {original_path} ({_format_bytes(file_size)})")
                restored_count += 1
                restored_size += file_size
        except OSError as e:
            logging.warning(f"Failed to process deleted marker {marker_path}: {e}")
    
    for temp_path, temp_name in temp_files.items():
        try:
            parts = temp_name[1:].rsplit('.', 1)
            if len(parts) == 2:
                original_name = parts[0]
                dir_path = os.path.dirname(temp_path)
                original_path = os.path.join(dir_path, original_name)
                deleted_path = original_path + ".deleted."
                
                has_deleted_marker = any(marker.startswith(deleted_path) for marker in deleted_markers.keys())
                
                if has_deleted_marker and not os.path.exists(original_path):
                    logging.debug(f"Keeping temp file (part of interrupted move): {temp_path}")
                    continue
                else:
                    file_size = os.path.getsize(temp_path)
                    if not dry_run:
                        os.remove(temp_path)
                    logging.info(f"{'Would clean' if dry_run else 'Cleaned'} orphaned temp file: {temp_path} ({_format_bytes(file_size)})")
                    cleaned_count += 1
                    cleaned_size += file_size
            else:
                file_size = os.path.getsize(temp_path)
                if not dry_run:
                    os.remove(temp_path)
                logging.info(f"{'Would clean' if dry_run else 'Cleaned'} unparseable temp file: {temp_path}")
                cleaned_count += 1
                cleaned_size += file_size
        except OSError as e:
            logging.warning(f"Failed to clean temp file {temp_path}: {e}")
    
    if restored_count > 0:
        logging.warning(f"Restored {restored_count} interrupted moves ({_format_bytes(restored_size)}) - files returned to source")
    if cleaned_count > 0:
        logging.info(f"Cleanup complete: {cleaned_count} orphaned files ({_format_bytes(cleaned_size)})")
    if cleaned_count == 0 and restored_count == 0:
        logging.debug("No orphaned temp files or deleted markers found")
    
    return cleaned_count, cleaned_size

