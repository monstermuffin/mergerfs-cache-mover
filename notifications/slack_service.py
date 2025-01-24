import logging
from typing import Dict, List, Any
import requests

class SlackService:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def _format_bytes(self, bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes < 1024:
                return f"{bytes:.2f}{unit}"
            bytes /= 1024
        return f"{bytes:.2f}PB"

    def send_completion(self, data: Dict[str, Any]) -> bool:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*🔄 Cache Move Complete*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Files Processed*\n"
                           f"{data['files_moved']:,}\n"
                           f"*💾 Data Moved*\n"
                           f"{data['space_moved']}\n\n"
                           f"*⏱️ Time Taken*\n"
                           f"{data['time_str']}\n"
                           f"*📈 Transfer Speed*\n"
                           f"{data['avg_speed']:.1f} MB/s\n\n"
                           f"*💽 Cache Status*\n"
                           f"Usage: {data['final_cache_usage']:.1f}% Used | {100 - data['final_cache_usage']:.1f}% Free\n"
                           f"Space: {data['cache_free_str']} Free of {data['cache_total_str']} Total\n"
                           f"*💾 Backing Status*\n"
                           f"Usage: {data['backing_usage']:.1f}% Used | {100 - data['backing_usage']:.1f}% Free\n"
                           f"Space: {data['backing_free_str']} Free of {data['backing_total_str']} Total"
                }
            }
        ]

        if data['commit_hash']:
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Version: {data['commit_hash'][:7]}"
                    }
                ]
            })

        return self._send_webhook({"blocks": blocks})

    def send_error(self, error_msg: str, commit_hash: str = None) -> bool:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*❌ Cache Mover Error*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error Details:*\n{error_msg}"
                }
            }
        ]

        if commit_hash:
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Version: {commit_hash[:7]}"
                    }
                ]
            })

        return self._send_webhook({"blocks": blocks})

    def send_threshold_not_met(self, current_usage: float, threshold: float, commit_hash: str = None,
                                  cache_free: int = None, cache_total: int = None,
                                  backing_free: int = None, backing_total: int = None) -> bool:
        message = f"Current cache usage ({current_usage:.1f}%) is below threshold ({threshold:.1f}%). No action required."

        if all(x is not None for x in [cache_free, cache_total, backing_free, backing_total]):
            cache_free_str = self._format_bytes(cache_free)
            cache_total_str = self._format_bytes(cache_total)
            backing_free_str = self._format_bytes(backing_free)
            backing_total_str = self._format_bytes(backing_total)

            message += f"\n\n*💽 Cache Status*\n"
            message += f"Space: {cache_free_str} Free of {cache_total_str} Total\n"
            message += f"\n*💾 Backing Status*\n"
            message += f"Space: {backing_free_str} Free of {backing_total_str} Total"

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*ℹ️ Cache Usage Update*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            }
        ]

        if commit_hash:
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Version: {commit_hash[:7]}"
                    }
                ]
            })

        return self._send_webhook({"blocks": blocks})
    
    def send_empty_cache(self, cache_free: int, cache_total: int,
                    backing_free: int, backing_total: int,
                    commit_hash: str = None) -> bool:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*ℹ️ Cache Empty Report*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "Empty cache mode activated but no files found!\n\n"
                        f"💽 Cache Status\n"
                        f"Space: {self._format_bytes(cache_free)} Free of {self._format_bytes(cache_total)} Total\n"
                        f"\n💾 Backing Status\n"
                        f"Space: {self._format_bytes(backing_free)} Free of {self._format_bytes(backing_total)} Total"
                    )
                }
            }
        ]
        if commit_hash:
            blocks.append({
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": f"Version: {commit_hash[:7]}"
                }]
            })
        return self._send_webhook({"blocks": blocks})

    def _send_webhook(self, payload: Dict[str, Any]) -> bool:
        try:
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            return True
        except Exception as e:
            logging.error(f"Failed to send Slack webhook: {str(e)}")
            return False