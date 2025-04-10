import logging
import shutil
from notifications import NotificationHandler as BaseNotificationHandler
from .updater import get_current_commit_hash

class NotificationManager:  
    def __init__(self, config):
        self.config = config
        self.enabled = config['Settings']['NOTIFICATIONS_ENABLED']
        self.urls = config['Settings']['NOTIFICATION_URLS']
        self.notify_threshold = config['Settings'].get('NOTIFY_THRESHOLD', False)
        
        self.commit_hash = get_current_commit_hash()
        
        handler_config = {
            'Settings': {
                'NOTIFICATIONS_ENABLED': self.enabled,
                'NOTIFICATION_URLS': self.urls,
                'NOTIFY_THRESHOLD': self.notify_threshold
            },
            'Paths': config.get('Paths', {})
        }
        self.handler = BaseNotificationHandler(handler_config, self.commit_hash)

    def _get_storage_stats(self):
        cache_path = self.config['Paths']['CACHE_PATH']
        backing_path = self.config['Paths']['BACKING_PATH']
        
        cache_total, cache_used, cache_free = shutil.disk_usage(cache_path)
        backing_total, backing_used, backing_free = shutil.disk_usage(backing_path)
        backing_usage = (backing_used / backing_total) * 100
        
        return {
            'cache_free': cache_free,
            'cache_total': cache_total,
            'backing_free': backing_free,
            'backing_total': backing_total,
            'backing_usage': backing_usage
        }

    def notify_threshold_not_met(self, current_usage, threshold):
        if not self.enabled or not self.notify_threshold:
            return

        stats = self._get_storage_stats()
        self.handler.notify_threshold_not_met(
            current_usage=current_usage,
            threshold=threshold,
            cache_free=stats['cache_free'],
            cache_total=stats['cache_total'],
            backing_free=stats['backing_free'],
            backing_total=stats['backing_total']
        )

    def notify_completion(self, moved_count, final_usage, total_bytes=0, elapsed_time=0, avg_speed=0):
        if not self.enabled:
            return

        stats = self._get_storage_stats()
        self.handler.notify_completion(
            files_moved=moved_count,
            total_bytes=total_bytes,
            elapsed_time=elapsed_time,
            final_usage=final_usage,
            cache_free=stats['cache_free'],
            cache_total=stats['cache_total'],
            backing_usage=stats['backing_usage'],
            backing_free=stats['backing_free'],
            backing_total=stats['backing_total'],
            avg_speed=avg_speed
        )

    def notify_error(self, error_message):
        if not self.enabled:
            return

        self.handler.notify_error(error_message)