"""
This just restarts the TimescaleDB background workers once a day,
in case they've stopped, for whatever reason
"""

import logging
import sys
from pathlib import Path

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


# local imports
from project.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "timescaledb_restart_background_workers"


def restart_background_workers_timescaledb(c) -> bool:
    """Restart the TimescaleDB background workers"""

    SQL = "SELECT _timescaledb_internal.start_background_workers();"
    run_query(c, SQL, db="timescale", commit=True)

    return True


@error_wrapper()
def main(c: Config) -> bool:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    is_good = restart_background_workers_timescaledb(c)

    return is_good


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/project/logs/"
    )
    main(c)
