# Configuration for the mover script

# Paths
CACHE_PATH = "/path/to/cache" #Path to cache disk !!not the cached pool!!
BACKING_PATH = "/path/to/backing" #Path to backing storage pool w/o cache

# Threshold
PERCENTAGE_THRESHOLD = 80  #Percentage at which the mover will start

# Webhook
USE_WEBHOOK = False  #Set to False to disable webhook notifications
WEBHOOK_URL = "https://your-webhook-url-here" #Webhook URL here

# Concurrency
MAX_WORKERS = 5  #Number of concurrent file moves. Adjust based on your system.