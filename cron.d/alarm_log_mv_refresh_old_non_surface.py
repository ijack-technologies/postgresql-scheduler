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
LOGFILE_NAME = 'alarm_log_refresh_mv_old_non_surface'

# SQL = "select refresh_alarm_log_mv_old_non_surface();"

# Requires owner privileges (must be run by "master" user, not "app_user")
SQL = """
    REFRESH MATERIALIZED VIEW CONCURRENTLY 
    public.alarm_log_mv_old_non_surface
    WITH DATA
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
