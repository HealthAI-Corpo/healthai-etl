from loguru import logger
import sys
import os


def configure_logging():
    logger.remove()

    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_dir = os.getenv("LOG_DIR", "./logs")
    log_file = os.getenv("LOG_FILE", "healthai_etl")
    retention = os.getenv("LOG_RETENTION_DAYS", "30")

    os.makedirs(log_dir, exist_ok=True)

    # Console
    logger.add(
        sys.stdout,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        enqueue=True,
    )

    # File
    logger.add(
        f"{log_dir}/{log_file}.log",
        level=log_level,
        rotation="00:00",
        retention=f"{retention} days",
        compression="zip",
        enqueue=True,
    )

    # Intercept standard logging
    import logging

    class InterceptHandler(logging.Handler):
        def emit(self, record):
            logger.opt(exception=record.exc_info).log(
                record.levelname, record.getMessage()
            )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
