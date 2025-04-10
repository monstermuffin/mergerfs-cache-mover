import logging
from logging.handlers import RotatingFileHandler

class HybridFormatter(logging.Formatter):
    def __init__(self, fmt="%(levelname)s: %(message)s"):
        super().__init__(fmt)

    def format(self, record):
        if hasattr(record, 'file_move'):
            return (f"{self.formatTime(record)} - {record.levelname} - File Move Operation:\n"
                    f"  From: {record.src}\n"
                    f"  To: {record.dest}\n"
                    f"  {record.msg}")
        else:
            return f"{self.formatTime(record)} - {record.levelname} - {record.msg}"

def setup_logging(config, console_log):
    log_formatter = HybridFormatter()
    log_handler = RotatingFileHandler(
        config['Paths']['LOG_PATH'],
        maxBytes=config['Settings']['MAX_LOG_SIZE_MB'] * 1024 * 1024,
        backupCount=config['Settings']['BACKUP_COUNT']
    )
    log_handler.setFormatter(log_formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    if console_log:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger 