import logging
import os
import platform
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# local imports
from utils import (
    Config, configure_logging, run_query, error_wrapper
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = 'alarm_log_delete_duplicates'

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


def main(c):
    """Main entrypoint function"""
    global SQL

    run_query(c, SQL)

    return None


if __name__ == '__main__':
    c = Config()
    c.logger = configure_logging(
        __name__,
        logfile_name = LOGFILE_NAME, 
        path_to_log_directory='/var/log/'
    )
    main(c)
