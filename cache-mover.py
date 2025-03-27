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
from cache_mover import __version__

def main():
    parser = argparse.ArgumentParser(description='MergerFS Cache Mover')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be moved without moving')
    parser.add_argument('--console-log', action='store_true', help='Log to console in addition to file')
    parser.add_argument('--version', action='version', version=f'MergerFS Cache Mover v{__version__}')
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Set up logging
    logger = setup_logging(config, args.console_log)

    # Check if another instance is running
    is_running, running_instances = is_script_running()
    if is_running:
        logger.error("Another instance is already running:")
        for instance in running_instances:
            logger.error(f"  {instance}")
        sys.exit(1)

    # Initialize managers
    notification_mgr = NotificationManager(config)
    cleanup_mgr = CleanupManager(config, args.dry_run)

    # Auto-update if enabled
    if config['Settings'].get('AUTO_UPDATE', False):
        if not auto_update(config):
            logger.warning("Auto-update failed or was skipped")

    try:
        # Check current cache usage
        current_usage, needs_cleanup = cleanup_mgr.check_usage()

        if not needs_cleanup and not args.dry_run:
            logger.info("Cache usage below threshold, nothing to do")
            notification_mgr.notify_threshold_not_met(current_usage, cleanup_mgr.threshold)
            sys.exit(0)

        # Set up signal handling
        def signal_handler(signum, frame):
            logger.info("Received signal to stop, finishing current operations...")
            cleanup_mgr.stop()
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Run cleanup
        result = cleanup_mgr.run_cleanup()
        if result:
            moved_count, final_usage, total_bytes, elapsed_time, avg_speed = result
            notification_mgr.notify_completion(
                moved_count=moved_count,
                final_usage=final_usage,
                total_bytes=total_bytes,
                elapsed_time=elapsed_time,
                avg_speed=avg_speed
            )

    except Exception as e:
        logger.error(f"Error during execution: {e}")
        notification_mgr.notify_error(str(e))
        raise

if __name__ == '__main__':
    main()