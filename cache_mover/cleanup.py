"""
Cleanup operations for the cache mover.
"""

import logging
from threading import Event
from .filesystem import (
    get_fs_usage,
    gather_files_to_move,
    remove_empty_dirs,
)
from .mover import move_files_concurrently

class CleanupManager:
    """Manages cleanup operations for the cache mover."""
    
    def __init__(self, config, dry_run=False):
        """
        Initialize the cleanup manager.
        
        Args:
            config (dict): Configuration dictionary
            dry_run (bool): If True, only simulate operations
        """
        self.config = config
        self.dry_run = dry_run
        self.stop_event = Event()
        self.cache_path = config['Paths']['CACHE_PATH']
        self.threshold = config['Settings']['THRESHOLD_PERCENTAGE']
        self.target = config['Settings']['TARGET_PERCENTAGE']

    def check_usage(self):
        """
        Check current cache usage against threshold.
        
        Returns:
            tuple: (current_usage, needs_cleanup)
        """
        current_usage = get_fs_usage(self.cache_path)
        needs_cleanup = (current_usage > self.threshold or 
                        (self.threshold == 0 and self.target == 0))
        
        logging.info(f"Current cache usage: {current_usage:.1f}%")
        logging.info(f"Threshold: {self.threshold}%, Target: {self.target}%")
        
        return current_usage, needs_cleanup

    def run_cleanup(self):
        """
        Run the cleanup operation.
        
        Returns:
            tuple: (moved_count, final_usage, total_bytes, elapsed_time, avg_speed) or None if no cleanup needed
        """
        # Gather files to move
        files_to_move = gather_files_to_move(self.config)
        regular_files, hardlink_groups = files_to_move
        total_files = len(regular_files) + sum(len(group) for group in hardlink_groups.values())
        
        if total_files == 0:
            logging.info("No files to move")
            return None

        logging.info(f"Found {total_files} files to move ({len(regular_files)} regular files, {sum(len(group) for group in hardlink_groups.values())} hardlinked files in {len(hardlink_groups)} groups)")
        
        # Move files
        moved_count, total_bytes, elapsed_time, avg_speed = move_files_concurrently(
            files_to_move,
            self.config,
            self.dry_run,
            self.stop_event
        )

        # Clean up empty directories if files were moved
        if not self.dry_run and moved_count > 0:
            removed_dirs = remove_empty_dirs(
                self.cache_path,
                self.config['Settings']['EXCLUDED_DIRS'],
                self.dry_run
            )
            if removed_dirs > 0:
                logging.info(f"Removed {removed_dirs} empty directories")

        # Get final usage
        final_usage = get_fs_usage(self.cache_path)
        return moved_count, final_usage, total_bytes, elapsed_time, avg_speed

    def stop(self):
        """Signal the cleanup operation to stop."""
        self.stop_event.set() 