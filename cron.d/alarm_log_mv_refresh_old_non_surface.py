import logging
import os
import platform
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# local imports
from utils import (
    Config, configure_logging, run_query, error_wrapper, send_mailgun_email, send_twilio_phone, send_twilio_sms
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

    try:
        # If we're testing the alerting (when an error happens), raise an exception
        if c.TEST_ERROR:
            raise ValueError

        run_query(c, SQL)

    except Exception as err:
        c.logger.exception(f"ERROR running program! Closing now... \nError msg: {err}")
        alertees_email = ['smccarthy@ijack.ca']
        alertees_sms = ['+14036897250']
        subject = f"IJACK {LOGFILE_NAME} ERROR!!!"
        msg_sms = f"Sean, check '{LOGFILE_NAME}.py' now! There has been an error!"
        msg_email = msg_sms + f"\nError message: {err}"

        message = send_twilio_sms(c, alertees_sms, msg_sms)
        rc = send_mailgun_email(c, text=msg_email, html='', emailees_list=alertees_email, subject=subject)

        c.TEST_DICT['message'] = message
        c.TEST_DICT['rc'] = rc
        c.TEST_DICT['msg_sms'] = msg_sms

        raise

    return None


if __name__ == '__main__':
    c = Config()
    c.logger = configure_logging(
        __name__,
        logfile_name = LOGFILE_NAME, 
        path_to_log_directory='/var/log/'
    )
    main(c)
