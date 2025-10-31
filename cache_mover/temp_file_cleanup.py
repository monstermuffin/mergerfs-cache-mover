import os
import re
import logging

from .filesystem import _format_bytes, is_excluded

TEMP_FILE_PATTERN = re.compile(r'^\..+\.[a-zA-Z0-9]{6}$')

def is_temp_file(filename):
    return TEMP_FILE_PATTERN.match(filename) is not None

def cleanup_orphaned_temp_files(backing_path, excluded_dirs, dry_run=False):
    cleaned_count = 0
    cleaned_size = 0
    
    logging.info("Scanning for orphaned temp files from previous runs")
    
    for root, dirs, files in os.walk(backing_path):
        dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d), excluded_dirs)] # LLM suggested fix
        
        for filename in files:
            if is_temp_file(filename):
                filepath = os.path.join(root, filename)
                try:
                    file_size = os.path.getsize(filepath)
                    if not dry_run:
                        os.remove(filepath)
                    logging.info(f"{'Would clean' if dry_run else 'Cleaned'} orphaned temp file: {filepath} ({_format_bytes(file_size)})")
                    cleaned_count += 1
                    cleaned_size += file_size
                except OSError as e:
                    logging.warning(f"Failed to clean temp file {filepath}: {e}")
    
    if cleaned_count > 0:
        logging.info(f"Cleanup complete: {cleaned_count} orphaned temp files ({_format_bytes(cleaned_size)})")
    else:
        logging.debug("No orphaned temp files found")
    
    return cleaned_count, cleaned_size

