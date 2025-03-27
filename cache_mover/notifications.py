"""
Notification handling for the cache mover.
"""

import logging
from notifications import NotificationHandler as BaseNotificationHandler

class NotificationManager:
    """Manages notifications for the cache mover."""
    
    def __init__(self, config):
        """
        Initialize the notification manager.
        
        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        self.enabled = config['Settings']['NOTIFICATIONS_ENABLED']
        self.urls = config['Settings']['NOTIFICATION_URLS']
        self.notify_threshold = config['Settings'].get('NOTIFY_THRESHOLD', False)
        
        # Create a config structure that matches what NotificationHandler expects
        handler_config = {
            'Settings': {
                'NOTIFICATIONS_ENABLED': self.enabled,
                'NOTIFICATION_URLS': self.urls,
                'NOTIFY_THRESHOLD': self.notify_threshold
            },
            'Paths': config.get('Paths', {})
        }
        self.handler = BaseNotificationHandler(handler_config)

    def notify_threshold_not_met(self, current_usage, threshold):
        """
        Send notification when cache usage is below threshold.
        
        Args:
            current_usage (float): Current cache usage percentage
            threshold (float): Threshold percentage
        """
        if not self.enabled or not self.notify_threshold:
            return

        self.handler.notify_threshold_not_met(
            current_usage=current_usage,
            threshold=threshold
        )

    def notify_completion(self, moved_count, final_usage):
        """
        Send notification when file moving is complete.
        
        Args:
            moved_count (int): Number of files moved
            final_usage (float): Final cache usage percentage
        """
        if not self.enabled:
            return

        # Get cache and backing storage stats
        cache_path = self.config['Paths']['CACHE_PATH']
        backing_path = self.config['Paths']['BACKING_PATH']
        
        import shutil
        cache_total, cache_used, cache_free = shutil.disk_usage(cache_path)
        backing_total, backing_used, backing_free = shutil.disk_usage(backing_path)
        backing_usage = (backing_used / backing_total) * 100

        self.handler.notify_completion(
            files_moved=moved_count,
            total_bytes=0,
            elapsed_time=0,
            final_usage=final_usage,
            cache_free=cache_free,
            cache_total=cache_total,
            backing_usage=backing_usage,
            backing_free=backing_free,
            backing_total=backing_total,
            avg_speed=0
        )

    def notify_error(self, error_message):
        """
        Send notification when an error occurs.
        
        Args:
            error_message (str): Error message to send
        """
        if not self.enabled:
            return

        self.handler.notify_error(error_message)