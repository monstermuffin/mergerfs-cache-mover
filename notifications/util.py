import logging
from typing import Any

import requests


# Source https://stackoverflow.com/a/1094933/5209106
def format_bytes(bytes: int | float, suffix: str = "B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(bytes) < 1024:
            return f"{bytes:3.1f} {unit}{suffix}"
        bytes /= 1024
    return f"{bytes:.1f} Yi{suffix}"

def send_webhook(service_name: str, webhook_url: str, payload: dict[str, Any]) -> bool:
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Failed to send {service_name} webhook: {str(e)}")
        return False
