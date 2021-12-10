import logging
import pathlib

# local imports
from cron_d.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "alarm_log_delete_duplicates"

# Delete duplicate records once a day (this table gets lots of duplicates if left alone)
SQL = """
    delete 
    FROM public.alarm_log
    WHERE timestamp_utc_inserted IN (
        SELECT timestamp_utc_inserted
        FROM (
            SELECT timestamp_utc_inserted,
            ROW_NUMBER() OVER ( 
                PARTITION BY timestamp_local, gateway, abbrev, value
                ORDER BY timestamp_utc_inserted 
            ) AS row_num
            FROM public.alarm_log 
        ) t
        WHERE t.row_num > 1 
    );
"""


@error_wrapper()
def main(c):
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, pathlib.Path(__file__).name)

    run_query(c, SQL, commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
