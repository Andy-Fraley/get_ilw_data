import logging
from .models import LoggingLevel
from .config import Config
from typing import Optional
import io

class EmailFilter(logging.Filter):
    """
    Filter for logging to capture only relevant messages for email notifications.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        if 'Completed backup' in record.msg or 'Size of backups' in record.msg or record.levelname in ('ERROR', 'CRITICAL'):
            return True
        else:
            return False

def setup_logging(config: Config, logging_level: str = LoggingLevel.warning.value) -> logging.Logger:
    """
    Set up logging for the application, including file, console, and string stream handlers.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.NOTSET)
    logging_formatter = logging.Formatter('%(asctime)s %(levelname)s\t%(message)s', '%Y-%m-%d %H:%M:%S')

    # File handler
    file_handler = logging.FileHandler(f"{config.prog_dir}/messages.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging_formatter)
    root_logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    logging_level_numeric = getattr(logging, logging_level, None)
    console_handler.setFormatter(logging_formatter)
    console_handler.setLevel(logging_level_numeric)
    root_logger.addHandler(console_handler)

    # String stream handler for error aggregation
    config.string_stream = config.string_stream or io.StringIO()
    string_handler = logging.StreamHandler(config.string_stream)
    string_handler.setFormatter(logging_formatter)
    string_handler.setLevel(logging.NOTSET)
    string_handler.addFilter(EmailFilter())
    root_logger.addHandler(string_handler)

    return root_logger 