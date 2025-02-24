import sys
import time
from pathlib import Path

import schedule

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project import (
    monitor_disk_space,
)
from project.logger_config import logger
from project.utils import Config


def make_schedule(c: Config) -> None:
    """Make a cron-like schedule for running tasks"""

    logger.info("Making the cron-like schedule for the EC2 monitoring jobs...")

    schedule.every().hour.at(":06").do(monitor_disk_space.monitor_disk_space_main, c=c)

    return None


def run_schedule() -> None:
    """
    Run the schedule of tasks and wait X seconds before running the scheduled tasks again.
    This loop will run forever.
    """

    # Configure the logger
    c = Config()

    # Make the schedule
    make_schedule(c=c)

    logger.info("App running ✅. Running scheduled tasks forever...")
    while True:
        # Run all scheduled tasks
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    run_schedule()
