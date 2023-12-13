import logging
import sys


def setup_logging(
    log_level: int = logging.INFO, logger_name: str = "logger"
) -> logging.Logger:
    """
    Create logger that logs to stdout.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s %(pathname)s:%(lineno)s  %(message)s",
    )
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    logger.addHandler(handler)
    return logger
