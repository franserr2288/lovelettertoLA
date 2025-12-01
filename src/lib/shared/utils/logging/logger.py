import logging
import os


def setup_logger(name: str = __name__, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    if logger.handlers:
        return logger
    
    is_local = os.getenv("ENVIRONMENT", "local") == "local"
    
    if is_local:
        formatter = logging.Formatter(
            '%(levelname)s | %(name)s:%(lineno)d | %(message)s'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
