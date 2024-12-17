import logging
from pathlib import Path

from logger_config import configure_logging
from utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

logger = logging.getLogger(__name__)

# Delete duplicate records once a day (this table gets lots of duplicates if left alone)
SQL = """
    delete 
    FROM public.alarm_log
    WHERE timestamp_utc_inserted IN (
        SELECT timestamp_utc_inserted
        FROM (
            SELECT timestamp_utc_inserted,
            ROW_NUMBER() OVER ( 
                PARTITION BY timestamp_local, power_unit, abbrev, value
                ORDER BY timestamp_utc_inserted 
            ) AS row_num
            FROM public.alarm_log 
        ) t
        WHERE t.row_num > 1 
    );
"""


@error_wrapper()
def main(c: Config) -> None:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    run_query(c, SQL, commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    configure_logging(__name__, logfile_name="alarm_log_delete_duplicates")
    main(c)
