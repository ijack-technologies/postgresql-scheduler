
import logging
import os
import platform
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

LOG_LEVEL = logging.INFO
# logger = logging.getLogger(__name__)


class Config():
    """Main config class"""
    pass


def configure_logging(name, logfile_name, path_to_log_directory='/var/log/'):
    """Configure logger"""
    global LOG_LEVEL

    logger = logging.getLogger(name)
    # Override the default logging.WARNING level so all messages can get through to the handlers
    logger.setLevel(logging.DEBUG) 
    formatter = logging.Formatter('%(asctime)s : %(module)s : %(lineno)d : %(levelname)s : %(funcName)s : %(message)s')

    date_for_log_filename = datetime.now().strftime('%Y-%m-%d')
    log_filename = f"{date_for_log_filename}_{logfile_name}.log"
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


def run_query(c, sql, db='ijack'):
    """Run and time the SQL query"""

    if db == 'ijack':
        host = os.getenv("HOST_IJ") 
        port = os.getenv("PORT_IJ") 
        dbname = os.getenv('DB_IJ')
        user = os.getenv("USER_IJ")
        password = os.getenv("PASS_IJ")
    elif db == 'timescale':
        host = os.getenv("HOST_TS") 
        port = os.getenv("PORT_TS") 
        dbname = os.getenv('DB_TS')
        user = os.getenv("USER_TS")
        password = os.getenv("PASS_TS")

    with psycopg2.connect(
        host=host, 
        port=port, 
        dbname=dbname, 
        user=user, 
        password=password, 
        connect_timeout=5
    ) as conn:
        with conn.cursor() as cursor:
            c.logger.info(f"Running query now... SQL to run: \n{sql}")
            time_start = time.time()
            cursor.execute(sql)
            time_finish = time.time()
            c.logger.info(f"Time to execute query: {round(time_finish - time_start)} seconds")

    return None


def error_wrapper(c, func, *args, **kwargs):
    """So the loop can continue even if a function fails"""

    try:
        func(*args, **kwargs)
    except Exception:
        c.logger.exception(f"Problem running function: {func}")
        # Keep going regardless
        pass 

    return None

