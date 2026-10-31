import logging
from typing import Union


def setup_logging(
    name: str = "ecommerce_analytics", level: Union[int, str] = logging.INFO
) -> logging.Logger:
    """
    Sets up a standardized logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create console handler and set its level
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    if not logger.handlers:
        logger.addHandler(ch)
    else:
        for handler in logger.handlers:
            handler.setLevel(level)

    return logger


def get_logger(name: str = "ecommerce_analytics") -> logging.Logger:
    """
    Gets a standardized logger (alias for setup_logging).
    """
    return setup_logging(name)
