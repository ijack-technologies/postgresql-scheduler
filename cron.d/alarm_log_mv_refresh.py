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
LOGFILE_NAME = 'alarm_log_mv_refresh'

# SQL = "select refresh_alarm_log_mv();"

# Requires owner privileges (must be run by "master" user, not "app_user")
SQL = """
    REFRESH MATERIALIZED VIEW CONCURRENTLY 
    public.alarm_log_mv
    WITH DATA
"""


# class ConfigDB:
#     """Make database connections and close them if something goes wrong"""
    
#     def __init__(self):
#         # Database connection 
#         self._db_connection_ij = psycopg2.connect(
#             host=os.getenv("HOST_IJ"), 
#             port=os.getenv("PORT_IJ"), 
#             dbname=os.getenv('DB_IJ'),
#             user=os.getenv("USER_IJ"), 
#             password=os.getenv("PASS_IJ"), 
#             connect_timeout=5
#         )
#         self._db_cursor_normal = self._db_connection_ij.cursor()
#         self._db_cursor_dict = self._db_connection_ij.cursor(cursor_factory=RealDictCursor)

#     def execute_only(self, sql):
#         time_start = time.time()
#         self._db_cursor_normal.execute(sql)
#         # data = self._db_cursor_normal.fetchall()
#         time_finish = time.time()
#         logger.info(f"Time to execute query: {round(time_finish - time_start)} seconds")
#         return None

#     def query_normal(self, sql):
#         time_start = time.time()
#         self._db_cursor_normal.execute(sql)
#         data = self._db_cursor_normal.fetchall()
#         time_finish = time.time()
#         logger.info(f"Time to execute query: {round(time_finish - time_start)} seconds")
#         return data

#     def query_dict(self, sql):
#         time_start = time.time()
#         self._db_cursor_dict.execute(sql)
#         data = self._db_cursor_dict.fetchall()
#         time_finish = time.time()
#         logger.info(f"Time to execute query: {round(time_finish - time_start)} seconds")
#         return data

#     def __del__(self):
#         self._db_cursor_dict.close()
#         self._db_cursor_normal.close()
#         self._db_connection_ij.close()
#         if self._db_connection_ij is not None:
#             del self._db_connection_ij
#         logger.info("Database connections closed")


# def alarm_log_refresh(db, minute_counter):
#     """Refresh the alarm log materialized view"""

#     global ALARM_LOG_REFRESH_EVERY_X_MINUTES

#     # If the remainder == 0, do the query 
#     # e.g. 0 % 5 == 0; 5 % 5 == 0; 10 % 5 == 0
#     if minute_counter % ALARM_LOG_REFRESH_EVERY_X_MINUTES == 0:
#         # sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY public.alarm_log_mv;"
#         sql = "select refresh_alarm_log_mv();"
#         logger.info("Refreshing alarm log materialized view...")
#         db.execute_only(sql)

#     return None


def main(c):
    """Main entrypoint function"""
    global SQL

    # while True:
    #     try:
    #         # Start the database connections
    #         db = ConfigDB()
    #         minute_counter = 0
    #         while True:
    #             # Refresh the alarm log materialized view
    #             error_wrapper(alarm_log_refresh, db, minute_counter)

    #             time.sleep(60) # Sleep 60 seconds
    #             minute_counter += 1


    #     except Exception:
    #         logger.exception("There's been a problem...")
    #         # Keep going regardless
    #         continue # continue: go back to the top

    # sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY public.alarm_log_mv;"

    try:
        # If we're testing the alerting (when an error happens), raise an exception
        if c.TEST_ERROR:
            raise ValueError

        run_query(c, SQL, commit=True)

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
