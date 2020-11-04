import logging
import os
import platform
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import pathlib

# local imports
from utils import (
    Config, configure_logging, run_query, error_wrapper, send_mailgun_email, send_twilio_phone, send_twilio_sms,
    exit_if_already_running,
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


@error_wrapper()
def main(c):
    """Main entrypoint function"""
    global SQL
    
    exit_if_already_running(c, pathlib.Path(__file__).name)

    run_query(c, SQL, commit=True)

    return None


if __name__ == '__main__':
    c = Config()
    c.logger = configure_logging(
        __name__,
        logfile_name = LOGFILE_NAME, 
        path_to_log_directory='/var/log/'
    )
    main(c)
