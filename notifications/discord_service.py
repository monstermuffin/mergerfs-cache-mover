from datetime import datetime
from typing import Dict, List, Any

from .util import format_bytes, send_webhook

class DiscordService:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_completion(self, data: Dict[str, Any]) -> bool:
        embeds = [{
            "title": "🔄 Cache Move Complete",
            "color": 0x00ff00,
            "fields": [
                {
                    "name": "📊 Files Processed",
                    "value": f"{data['files_moved']:,}",
                    "inline": True
                },
                {
                    "name": "💾 Data Moved",
                    "value": data['space_moved'],
                    "inline": True
                },
                {
                    "name": "\u200b",
                    "value": "\u200b",
                    "inline": True
                },
                {
                    "name": "⏱️ Time Taken",
                    "value": data['time_str'],
                    "inline": True
                },
                {
                    "name": "📈 Transfer Speed",
                    "value": f"{data['avg_speed']:.1f} MB/s",
                    "inline": True
                },
                {
                    "name": "\u200b",
                    "value": "\u200b",
                    "inline": True
                },
                {
                    "name": "💽 Cache Status",
                    "value": (f"**Usage:** {data['final_cache_usage']:.1f}% Used | {100 - data['final_cache_usage']:.1f}% Free\n"
                            f"**Space:** {data['cache_free_str']} Free of {data['cache_total_str']} Total"),
                    "inline": False
                },
                {
                    "name": "💾 Backing Status",
                    "value": (f"**Usage:** {data['backing_usage']:.1f}% Used | {100 - data['backing_usage']:.1f}% Free\n"
                            f"**Space:** {data['backing_free_str']} Free of {data['backing_total_str']} Total\n"
                            f"**Path:** {data['backing_path']}"),
                    "inline": False
                }
            ],
            "footer": {
                "text": f"Version: {data['commit_hash'][:7] if data['commit_hash'] else 'unknown'}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }]

        return send_webhook("Discord", self.webhook_url, {"embeds": embeds})

    def send_error(self, error_msg: str, commit_hash: str = None) -> bool:
        embeds = [{
            "title": "❌ Cache Mover Error",
            "color": 0xff0000,
            "description": error_msg,
            "footer": {
                "text": f"Version: {commit_hash[:7] if commit_hash else 'unknown'}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }]

        return send_webhook("Discord", self.webhook_url, {"embeds": embeds})

    def send_threshold_not_met(self, current_usage: float, threshold: float, commit_hash: str = None,
                                  cache_free: int = None, cache_total: int = None,
                                  backing_free: int = None, backing_total: int = None) -> bool:
        description = f"Current cache usage ({current_usage:.1f}%) is below threshold ({threshold:.1f}%). No action required."

        if all(x is not None for x in [cache_free, cache_total, backing_free, backing_total]):
            cache_free_str = format_bytes(cache_free)
            cache_total_str = format_bytes(cache_total)
            backing_free_str = format_bytes(backing_free)
            backing_total_str = format_bytes(backing_total)

            description += f"\n\n**💽 Cache Status**\n"
            description += f"Space: {cache_free_str} Free of {cache_total_str} Total\n"
            description += f"\n**💾 Backing Status**\n"
            description += f"Space: {backing_free_str} Free of {backing_total_str} Total"

        embeds = [{
            "title": "ℹ️ Cache Usage Update",
            "color": 0x3498db,
            "description": description,
            "footer": {
                "text": f"Version: {commit_hash[:7] if commit_hash else 'unknown'}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }]

        return send_webhook("Discord", self.webhook_url, {"embeds": embeds})

    def send_empty_cache(self, cache_free: int, cache_total: int,
                    backing_free: int, backing_total: int,
                    commit_hash: str = None) -> bool:
        embeds = [{
            "title": "ℹ️ Cache Empty Report",
            "color": 0x3498db,
            "description": (
                "Empty cache mode activated but no files found!\n\n"
                f"💽 Cache Status\n"
                f"Space: {format_bytes(cache_free)} Free of {format_bytes(cache_total)} Total\n"
                f"\n💾 Backing Status\n"
                f"Space: {format_bytes(backing_free)} Free of {format_bytes(backing_total)} Total"
            ),
            "footer": {
                "text": f"Version: {commit_hash[:7] if commit_hash else 'unknown'}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }]
        return send_webhook("Discord", self.webhook_url, {"embeds": embeds})
