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
