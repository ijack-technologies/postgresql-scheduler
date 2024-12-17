import logging
from pathlib import Path

from project.logger_config import configure_logging
from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

logger = logging.getLogger(__name__)

# Delete old data from the time_series_rt table
SQL = """
    delete 
    FROM public.time_series_rt
    WHERE timestamp_utc < now() - interval '1 year'
;
"""


@error_wrapper()
def main(c: Config) -> None:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    run_query(c=c, sql=SQL, db="timescale", commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    configure_logging(__name__, logfile_name="time_series_rt_delete_old_data")
    main(c)
