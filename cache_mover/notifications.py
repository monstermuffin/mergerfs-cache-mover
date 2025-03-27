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
        self.enabled = config['Settings']['NOTIFICATIONS_ENABLED']
        self.urls = config['Settings']['NOTIFICATION_URLS']
        self.notify_threshold = config['Settings'].get('NOTIFY_THRESHOLD', False)
        self.handler = BaseNotificationHandler(self.enabled, self.urls)

    def notify_threshold_not_met(self, current_usage, threshold):
        """
        Send notification when cache usage is below threshold.
        
        Args:
            current_usage (float): Current cache usage percentage
            threshold (float): Threshold percentage
        """
        if not self.enabled or not self.notify_threshold:
            return

        self.handler.send_notification(
            "Cache Mover Status",
            f"Cache usage ({current_usage:.1f}%) below threshold ({threshold}%), nothing to do"
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

        self.handler.send_notification(
            "Cache Mover Complete",
            f"Moved {moved_count} files. Cache usage: {final_usage:.1f}%"
        )

    def notify_error(self, error_message):
        """
        Send notification when an error occurs.
        
        Args:
            error_message (str): Error message to send
        """
        if not self.enabled:
            return

        self.handler.send_notification(
            "Cache Mover Error",
            f"An error occurred: {error_message}"
        )