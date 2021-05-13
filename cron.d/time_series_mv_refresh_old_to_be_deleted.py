import logging
import pathlib

# local imports
from utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "time_series_mv_refresh"

# Requires owner privileges (must be run by "master" user, not "app_user")
SQL1 = """
    REFRESH MATERIALIZED VIEW CONCURRENTLY 
    public.time_series_view
    WITH DATA;
"""
SQL2 = """
    REFRESH MATERIALIZED VIEW CONCURRENTLY 
    public.time_series_mv
    WITH DATA;
"""

# SQL = "select refresh_time_series_mv();"


@error_wrapper()
def main(c):
    """Main entrypoint function"""

    exit_if_already_running(c, pathlib.Path(__file__).name)

    run_query(c, SQL1, db="timescale", commit=True)
    run_query(c, SQL2, db="timescale", commit=True)

    return True


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
