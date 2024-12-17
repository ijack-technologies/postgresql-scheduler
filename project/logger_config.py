import logging
import sys
import platform
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def configure_logging(
    name: str,
    logfile_name: str,
    log_directory: str = "/project/logs/",
    log_level: int = logging.INFO,
    want_file_handler: bool = True,
) -> None:
    """Configure logger"""

    path_to_log_directory: Path = Path(log_directory)
    path_to_log_directory.mkdir(parents=True, exist_ok=True)

    # Configure root logger. The root logger's handlers (in our case, both the file and console handlers)
    # are automatically inherited by all child loggers due to Python's logger propagation system.
    root_logger = logging.getLogger(name)
    # Override the default logging.WARNING level so all messages can get through to the handlers
    root_logger.setLevel(logging.DEBUG)
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Configure logger
    formatter = logging.Formatter(
        "%(asctime)s : %(module)s : %(lineno)d : %(levelname)s : %(funcName)s : %(message)s"
    )

    # Console handler (stdout) - crucial for Docker logs
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    if want_file_handler and platform.system() == "Linux":
        log_filename = f"{logfile_name}.log"
        log_filepath = path_to_log_directory.joinpath(log_filename)
        # file_handler = logging.FileHandler(filename=log_filepath)
        file_handler = TimedRotatingFileHandler(
            filename=log_filepath,
            when="H",
            interval=1,
            backupCount=48,
            encoding=None,
            delay=False,
            utc=False,
            atTime=None,
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        root_logger.info("Added fileHandler to logger: %s", log_filepath)

    # Add handlers
    root_logger.addHandler(console_handler)

    root_logger.info("Finished configuring the logger(s)")

    return None
