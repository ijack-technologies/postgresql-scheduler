import logging
import os
import platform
# import boto3
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

LOG_LEVEL = logging.INFO
# ALARM_LOG_REFRESH_EVERY_X_MINUTES = 5


def configure_logging(name, path_to_log_directory='/var/log/'):
    """Configure logger"""
    global LOG_LEVEL

    logger = logging.getLogger(name)
    # Override the default logging.WARNING level so all messages can get through to the handlers
    logger.setLevel(logging.DEBUG) 
    formatter = logging.Formatter('%(asctime)s : %(module)s : %(lineno)d : %(levelname)s : %(funcName)s : %(message)s')

    date_for_log_filename = datetime.now().strftime('%Y-%m-%d') + '_'
    log_filename = f"{date_for_log_filename}_db_refresh.log"
    log_filepath = os.path.join(path_to_log_directory, log_filename)

    if platform.system() == 'Linux':
        fh = logging.FileHandler(filename=log_filepath)
        fh.setLevel(LOG_LEVEL)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # sh = logging.StreamHandler(sys.stdout)
    sh = logging.StreamHandler()
    sh.setLevel(LOG_LEVEL)
    sh.setFormatter(formatter)
    # print(f"logger.handlers before adding streamHandler: {logger.handlers}")
    logger.addHandler(sh) 
    # print(f"logger.handlers after adding streamHandler: {logger.handlers}")

    # Test logger
    sh.setLevel(logging.DEBUG)
    logger.debug(f"Testing the logger: platform.system() = {platform.system()}")
    sh.setLevel(LOG_LEVEL)
    
    return logger


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


# def error_wrapper(func, *args, **kwargs):
#     """So the loop can continue even if a function fails"""

#     try:
#         func(*args, **kwargs)
#     except Exception:
#         logger.exception(f"Problem running function: {func}")
#         # Keep going regardless
#         pass 

#     return None


def main():
    """Main entrypoint function"""

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

    with psycopg2.connect(
            host=os.getenv("HOST_IJ"), 
            port=os.getenv("PORT_IJ"), 
            dbname=os.getenv('DB_IJ'),
            user=os.getenv("USER_IJ"), 
            password=os.getenv("PASS_IJ"), 
            connect_timeout=5
        ) as conn:
        with conn.cursor() as cursor:
            # sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY public.alarm_log_mv;"
            sql = "select refresh_alarm_log_mv();"
            logger.info("Refreshing alarm log materialized view...")


    return None


if __name__ == '__main__':
    logger = configure_logging(__name__)
    main()
