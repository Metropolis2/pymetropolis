import sys

from loguru import logger


def setup():
    logger.remove()
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> |"
            " <level>{message}</level>"
        ),
        backtrace=False,
        diagnose=False,
    )
