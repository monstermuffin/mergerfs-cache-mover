#!/usr/bin/env python3

import argparse
import logging
import signal
import sys

from cache_mover.config import load_config
from cache_mover.logging_setup import setup_logging
from cache_mover.filesystem import is_script_running
from cache_mover.updater import auto_update
from cache_mover.notifications import NotificationManager
from cache_mover.cleanup import CleanupManager
from cache_mover.temp_file_cleanup import cleanup_orphaned_temp_files
from cache_mover import __version__

def display_art():
    art = """
◢◤◢◤◢◤ ███╗░░░███╗░█████╗░███╗░░██╗░██████╗ ◢◤◢◤◢◤
◢◤◢◤◢◤ ████╗░████║██╔══██╗████╗░██║██╔════╝ ◢◤◢◤◢◤
◢◤◢◤◢◤ ██╔████╔██║███████║██╔██╗██║╚█████╗░ ◢◤◢◤◢◤
◢◤◢◤◢◤ ██║╚██╔╝██║██╔══██║██║╚████║░╚═══██╗ ◢◤◢◤◢◤
◢◤◢◤◢◤ ██║░╚═╝░██║██║░░██║██║░╚███║██████╔╝ ◢◤◢◤◢◤
◢◤◢◤◢◤ ╚═╝░░░░░╚═╝╚═╝░░╚═╝╚═╝░░╚══╝╚═════╝░ ◢◤◢◤◢◤
"""
    print(art)

def main():
    display_art()

    parser = argparse.ArgumentParser(description='MergerFS Cache Mover')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be moved without moving')
    parser.add_argument('--console-log', action='store_true', help='Log to console in addition to file')
    parser.add_argument('--config', help='Path to config)')
    parser.add_argument('--version', action='version', version=f'MergerFS Cache Mover v{__version__}')
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    logger = setup_logging(config, args.console_log)

    instance_id = config['Settings'].get('INSTANCE_ID')
    is_running, running_instances = is_script_running(instance_id)
    if is_running:
        if instance_id:
            logger.error(f"Another instance with ID '{instance_id}' is already running:")
        else:
            logger.error("Another instance is already running:")
        for instance in running_instances:
            logger.error(f"  {instance}")
        sys.exit(1)

    notification_mgr = NotificationManager(config)
    cleanup_mgr = CleanupManager(config, args.dry_run)

    if config['Settings'].get('AUTO_UPDATE', False):
        if not auto_update(config):
            logger.warning("Auto-update failed or was skipped")

    if config['Settings'].get('USE_TEMP_FILES', False) and config['Settings'].get('CLEANUP_TEMP_FILES_ON_START', True):
        cleanup_orphaned_temp_files(config['Paths']['BACKING_PATH'], dry_run=args.dry_run)

    try:
        current_usage, needs_cleanup = cleanup_mgr.check_usage()

        # Fix: Empty cache mode should still notify if NOTIFY_THRESHOLD
        empty_cache_mode = (cleanup_mgr.threshold == 0 and cleanup_mgr.target == 0)
        
        if not args.dry_run and not needs_cleanup:
            logger.info("Cache usage below threshold")
            notification_mgr.notify_threshold_not_met(current_usage, cleanup_mgr.threshold)
            sys.exit(0)

        def signal_handler(signum, frame):
            logger.info("Received signal to stop, finishing current operations...")
            cleanup_mgr.stop()
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        result = cleanup_mgr.run_cleanup()
        if not args.dry_run:
            if result:
                moved_count, final_usage, total_bytes, elapsed_time, avg_speed = result
                notification_mgr.notify_completion(
                    moved_count=moved_count,
                    final_usage=final_usage,
                    total_bytes=total_bytes,
                    elapsed_time=elapsed_time,
                    avg_speed=avg_speed
                )
            elif empty_cache_mode:
                # Fix: Empty cache mode should still notify if NOTIFY_THRESHOLD
                logger.info("Empty cache mode: no files found to move")
                notification_mgr.notify_threshold_not_met(current_usage, cleanup_mgr.threshold)
        elif args.dry_run and result:
            logger.info("DRY RUN: Skipping notification")

    except Exception as e:
        logger.error(f"Error during execution: {e}")
        notification_mgr.notify_error(str(e))
        raise

if __name__ == '__main__':
    main()
