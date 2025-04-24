import apprise
import logging
import platform
from typing import Any
from dataclasses import dataclass
from urllib.parse import urlparse
from .discord_service import DiscordService
from .slack_service import SlackService
from .util import format_bytes

@dataclass
class NotificationConfig:
    enabled: bool
    urls: list[str]
    hostname: str = platform.node()
    commit_hash: str | None = None
    notify_threshold: bool = False
    backing_path: str | None = None

class NotificationHandler:
    def __init__(self, config: dict[str, Any], commit_hash: str | None = None):
        settings = config.get('Settings', {})

        self.config = NotificationConfig(
            enabled=settings.get('NOTIFICATIONS_ENABLED', False),
            urls=settings.get('NOTIFICATION_URLS', []),
            commit_hash=commit_hash,
            backing_path=config.get('Paths', {}).get('BACKING_PATH'),
            notify_threshold=settings.get('NOTIFY_THRESHOLD', False)
        )
        
        self.apobj = None
        self.discord_services: list[DiscordService] = []
        self.slack_services: list[SlackService] = []
        
        if self.config.enabled and self.config.urls:
            self.apobj = apprise.Apprise()
            
            for url in self.config.urls:
                if url.startswith('discord'):
                    webhook_url = self._convert_discord_url(url)
                    if webhook_url:
                        self.discord_services.append(DiscordService(webhook_url))
                elif url.startswith('slack'):
                    webhook_url = self._convert_slack_url(url)
                    if webhook_url:
                        self.slack_services.append(SlackService(webhook_url))
                else:
                    self.apobj.add(url) # type: ignore

    def _calculate_percentage(self, used: int, total: int) -> float:
        if total == 0:
            return 0.0
        return (used / total) * 100
        
    def _format_time(self, seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"

    def _convert_discord_url(self, apprise_url: str) -> str | None:
        try:
            parsed = urlparse(apprise_url)
            if parsed.scheme != 'discord':
                return None
            webhook_id = parsed.hostname
            webhook_token = parsed.path.lstrip('/')
            return f"https://discord.com/api/webhooks/{webhook_id}/{webhook_token}"
        except Exception as e:
            logging.error(f"Failed to convert Discord Apprise URL: {str(e)}")
            return None
            
    def _convert_slack_url(self, apprise_url: str) -> str | None:
        try:
            parsed = urlparse(apprise_url)
            if parsed.scheme != 'slack':
                return None
            tokens = parsed.path.lstrip('/').split('/')
            if len(tokens) != 3:
                return None
            return f"https://hooks.slack.com/services/{tokens[0]}/{tokens[1]}/{tokens[2]}"
        except Exception as e:
            logging.error(f"Failed to convert Slack Apprise URL: {str(e)}")
            return None

    def notify_completion(self, files_moved: int, total_bytes: int, 
                        elapsed_time: float, final_usage: float,
                        cache_free: int, cache_total: int,
                        backing_usage: float, backing_free: int, 
                        backing_total: int,
                        avg_speed: float) -> bool:
        if not self.config.enabled:
            return False
            
        logging.info("Sending completion notification")
        
        # Calculate cache usage in GiB
        cache_used = cache_total - cache_free
        cache_usage = self._calculate_percentage(cache_used, cache_total)
        
        # Calculate backing usage in GiB
        backing_used = backing_total - backing_free
        backing_usage = self._calculate_percentage(backing_used, backing_total)
        
        notification_data = {
            'files_moved': files_moved,
            'space_moved': format_bytes(total_bytes),
            'time_str': self._format_time(elapsed_time),
            'avg_speed': avg_speed,
            'final_cache_usage': cache_usage,
            'cache_free_str': format_bytes(cache_free),
            'cache_total_str': format_bytes(cache_total),
            'backing_usage': backing_usage,
            'backing_free_str': format_bytes(backing_free),
            'backing_total_str': format_bytes(backing_total),
            'backing_path': self.config.backing_path,
            'commit_hash': self.config.commit_hash
        }
        
        success = True
        
        for service in self.discord_services:
            if not service.send_completion(notification_data):
                success = False
        
        for service in self.slack_services:
            if not service.send_completion(notification_data):
                success = False
        
        if self.apobj:
            message = (
                "✅ Cache Move Complete\n\n"
                f"**Files Moved:** {files_moved:,}\n"
                f"**Space Moved:** {notification_data['space_moved']}\n"
                f"**Time Taken:** {notification_data['time_str']}\n"
                f"**Average Speed:** {avg_speed:.1f}MB/s\n\n"
                f"**Cache Usage:** {final_usage:.1f}%\n"
                f"**Backing Storage:** {backing_usage:.1f}% Used "
                f"({notification_data['backing_free_str']} Free of {notification_data['backing_total_str']})"
            )
            
            try:
                if not self.apobj.notify( # type: ignore
                    title="Cache Move Complete",
                    body=message,
                    body_format=apprise.NotifyFormat.MARKDOWN
                ):
                    success = False
            except Exception as e:
                logging.error(f"Failed to send Apprise notification: {str(e)}")
                success = False
                
        return success

    def notify_error(self, error_msg: str) -> bool:
        if not self.config.enabled:
            return False
            
        success = True
        
        for service in self.discord_services:
            if not service.send_error(error_msg, self.config.commit_hash):
                success = False
        
        for service in self.slack_services:
            if not service.send_error(error_msg, self.config.commit_hash):
                success = False
        
        if self.apobj:
            message = f"❌ Cache Mover Error\n\n**Error Details:** {error_msg}"
            
            try:
                if not self.apobj.notify( # type: ignore
                    title="Cache Mover Error",
                    body=message,
                    body_format=apprise.NotifyFormat.MARKDOWN
                ):
                    success = False
            except Exception as e:
                logging.error(f"Failed to send Apprise notification: {str(e)}")
                success = False
                
        return success

    def notify_threshold_not_met(
        self,
        current_usage: float,
        threshold: float,
        cache_free: int | None = None,
        cache_total: int | None = None,
        backing_free: int | None = None,
        backing_total: int | None = None,
    ) -> bool:
        if not self.config.enabled or not self.config.notify_threshold:
            return False
                
        success = True
        
        for service in self.discord_services:
            if not service.send_threshold_not_met(current_usage, threshold, self.config.commit_hash,
                                                cache_free=cache_free, cache_total=cache_total,
                                                backing_free=backing_free, backing_total=backing_total):
                success = False
        
        for service in self.slack_services:
            if not service.send_threshold_not_met(current_usage, threshold, self.config.commit_hash,
                                                cache_free=cache_free, cache_total=cache_total,
                                                backing_free=backing_free, backing_total=backing_total):
                success = False
        
        if self.apobj:
            message = (
                "ℹ️ Cache Usage Update\n\n"
                f"Current cache usage ({current_usage:.1f}%) is below threshold ({threshold:.1f}%). "
                "No action required."
            )

            if all(x is not None for x in [cache_free, cache_total, backing_free, backing_total]):
                # The type checker doesn't recognise that the condition above guarantees these variables won't be None, hence this hack
                assert cache_free is not None ; assert cache_total is not None ; assert backing_free is not None ; assert backing_total is not None  # noqa: E702
                message += (f"\n\n💽 Cache Status\n"
                        f"Space: {format_bytes(cache_free)} Free of {format_bytes(cache_total)} Total\n"
                        f"\n💾 Backing Status\n"
                        f"Space: {format_bytes(backing_free)} Free of {format_bytes(backing_total)} Total")
            
            try:
                if not self.apobj.notify( # type: ignore
                    title="Cache Usage Update",
                    body=message,
                    body_format=apprise.NotifyFormat.MARKDOWN
                ):
                    success = False
            except Exception as e:
                logging.error(f"Failed to send Apprise notification: {str(e)}")
                success = False
                
        return success
    
    def notify_empty_cache(self, cache_free: int, cache_total: int,
                        backing_free: int, backing_total: int) -> bool:
        if not self.config.enabled:
            return False

        success = True
        message = (
            "ℹ️ Cache Empty Report\n\n"
            "Empty cache mode activated but no files found!\n\n"
            f"💽 Cache Status\n"
            f"Space: {format_bytes(cache_free)} Free of {format_bytes(cache_total)} Total\n"
            f"\n💾 Backing Status\n"
            f"Space: {format_bytes(backing_free)} Free of {format_bytes(backing_total)} Total"
        )

        for service in self.discord_services:
            if not service.send_empty_cache(
                cache_free, cache_total,
                backing_free, backing_total,
                self.config.commit_hash
            ):
                success = False

        for service in self.slack_services:
            if not service.send_empty_cache(
                cache_free, cache_total,
                backing_free, backing_total,
                self.config.commit_hash
            ):
                success = False

        # Send via Apprise
        if self.apobj:
            try:
                self.apobj.notify( # type: ignore
                    title="Cache Empty Report",
                    body=message,
                    body_format=apprise.NotifyFormat.MARKDOWN
                )
            except Exception as e:
                logging.error(f"Empty cache notification failed: {str(e)}")
                success = False

        return success
