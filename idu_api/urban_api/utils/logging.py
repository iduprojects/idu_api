import logging
import sys
from typing import Literal

import structlog

LoggingLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def configure_logging(
    log_level: LoggingLevel, files: dict[str, LoggingLevel] | None = None, root_logger_level: LoggingLevel = "INFO"
) -> structlog.stdlib.BoundLogger:
    level_name_mapping = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    files = files or {}

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logger: structlog.stdlib.BoundLogger = structlog.get_logger("main")
    logger.setLevel(level_name_mapping[log_level])

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(processor=structlog.dev.ConsoleRenderer(colors=True))
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)

    for filename, level in files.items():
        file_handler = logging.FileHandler(filename=filename, encoding="utf-8")
        file_handler.setFormatter(structlog.stdlib.ProcessorFormatter(processor=structlog.processors.JSONRenderer()))
        file_handler.setLevel(level_name_mapping[level])
        root_logger.addHandler(file_handler)

    root_logger.setLevel(root_logger_level)

    return logger


def get_handler_from_path(path: str) -> str:
    parts = path.split("/")
    return "/".join(part if not part.rstrip(".0").isdigit() else "*" for part in parts)
