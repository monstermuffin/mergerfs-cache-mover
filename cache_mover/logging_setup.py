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
    
    log_level_name = str(config['Settings'].get('LOG_LEVEL', 'INFO')).upper()
    
    if log_level_name not in logging._nameToLevel:
        logging.warning(f"Invalid log level '{log_level_name}', defaulting to INFO")
        log_level_name = 'INFO'
    
    logger.setLevel(log_level_name)
    
    logger.addHandler(log_handler)

    if console_log:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger 