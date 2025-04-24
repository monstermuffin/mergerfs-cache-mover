import logging
import requests

def format_bytes(self, bytes: int) -> str:
    gib = bytes / (1024**3)
    return f"{gib:.2f}GiB"

def send_webhook(service_name: str, webhook_url: str, payload: Dict[str, Any]) -> bool:
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Failed to send {service_name} webhook: {str(e)}")
        return False
