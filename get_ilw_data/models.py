from enum import Enum

class LoggingLevel(str, Enum):
    """
    Enum for logging levels.
    """
    debug = 'DEBUG'
    info = 'INFO'
    warning = 'WARNING'
    error = 'ERROR'
    critical = 'CRITICAL' 