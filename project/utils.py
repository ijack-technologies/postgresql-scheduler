"""
Utility functions and configuration for the PostgreSQL Scheduler application.

This module provides core infrastructure used across all scheduled jobs:
- Config class with database connections, email/SMS lists, and environment settings
- Database connection management with context managers (PostgreSQL, TimescaleDB)
- AWS IoT client for device shadow operations
- Email alerting via Mailgun (send_mailgun_email)
- SMS alerting via Twilio (send_twilio_sms)
- Error handling with email/SMS notifications (error_wrapper decorator)
- Process management (exit_if_already_running to prevent duplicate job execution)
- Datetime utilities for UTC and timezone conversions
- Query execution helpers with transaction support

All scheduled jobs import from this module to maintain DRY principles and consistent
error handling across the application.
"""

import functools
import json
import logging
import os
import random
import signal
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from datetime import time as dt_time
from pathlib import Path
from subprocess import PIPE, STDOUT
from typing import Generator, List, Tuple
from unittest.mock import MagicMock

import boto3
import pandas as pd
import psycopg2
import pytz
import requests
from psycopg2.extras import DictCursor, RealDictCursor
from twilio.rest import Client
from twilio.rest.api.v2010.account.message import MessageInstance

from project.logger_config import logger


class Config:
    """Main config class"""

    TEST_FUNC = False
    TEST_ERROR = False
    DEV_TEST_PRD = "production"
    PHONE_LIST_DEV = ["+14036897250"]
    EMAIL_LIST_DEV = ["smccarthy@myijack.com"]
    EMAIL_LIST_SERVICE_PRODUCTION_IT = [
        "rbarry@myijack.com",
        "gmannle@myijack.com",
        "smccarthy@myijack.com",
        "msenicar@myijack.com",
    ]
    EMAIL_LIST_OP_HOURS = [
        "public@myijack.com",
        # "everyone@myijack.com",
        "smccarthy@myijack.com",
        # "rbarry@myijack.com",
        # "gmannle@myijack.com",
        # "msenicar@myijack.com",
        # "dmccarthy@myijack.com",
        # "tbeals@myijack.com",
    ]
    # For returning values in the "c" config object
    TEST_DICT = {}


# NOTE the below datetime functions are adapted from this Miguel Grinberg blog post:
# https://blog.miguelgrinberg.com/post/it-s-time-for-a-change-datetime-utcnow-is-now-deprecated


def utcnow_aware() -> datetime:
    """Get the current time in UTC, with timezone info attached"""
    return datetime.now(timezone.utc)


def utcnow_naive() -> datetime:
    """Get the current time in UTC, without timezone info"""
    return utcnow_aware().replace(tzinfo=None)


def utcfromtimestamp_aware(timestamp: float) -> datetime:
    """Convert a timestamp to a UTC datetime object with timezone info attached"""
    return datetime.fromtimestamp(timestamp, timezone.utc)


def utcfromtimestamp_naive(timestamp: float) -> datetime:
    """Convert a timestamp to a UTC datetime object without timezone info"""
    return utcfromtimestamp_aware(timestamp).replace(tzinfo=None)


@contextmanager
def get_conn(
    db: str = "aws_rds", options_dict: dict | None = None, cursor_factory=None
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Get connection to IJACK database"""

    if db in ("ijack", "aws_rds"):
        host = os.getenv("HOST_IJ")
        port = int(os.getenv("PORT_IJ"))
        dbname = os.getenv("DB_IJ")
        user = os.getenv("USER_IJ")
        password = os.getenv("PASS_IJ")
    elif db == "timescale":
        host = os.getenv("HOST_TS")
        port = int(os.getenv("PORT_TS"))
        dbname = os.getenv("DB_TS")
        user = os.getenv("USER_TS")
        password = os.getenv("PASS_TS")
    elif db == "timescale_old":
        host = os.getenv("HOST_TS_OLD")
        port = int(os.getenv("PORT_TS_OLD"))
        dbname = os.getenv("DB_TS_OLD")
        user = os.getenv("USER_TS_OLD")
        password = os.getenv("PASS_TS_OLD")
    else:
        raise ValueError("db must be one of 'ijack', 'aws_rds', or 'timescale'")

    options_dict = options_dict or {
        "connect_timeout": 10,
        # whether client-side TCP keepalives are used
        "keepalives": 1,
        # seconds of inactivity after which TCP should send a keepalive message to the server
        "keepalives_idle": 60,
        # seconds after which a TCP keepalive message that is not acknowledged by the server should be retransmitted
        "keepalives_interval": 15,
        # TCP keepalives that can be lost before the client's connection to the server is considered dead
        "keepalives_count": 5,
        # milliseconds that transmitted data may remain unacknowledged before a connection is forcibly closed
        "tcp_user_timeout": 60000,
    }
    options_dict["cursor_factory"] = cursor_factory or DictCursor

    # AWS RDS requires SSL; TimescaleDB on EC2 does not
    sslmode = "require" if db in ("ijack", "aws_rds") else "prefer"

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        **options_dict,
    )

    try:
        yield conn
    except Exception:
        logger.exception("ERROR with database connection!")
        # Only rollback if connection is still open (handles SSL closure gracefully)
        if not conn.closed:
            try:
                conn.rollback()
            except Exception as rollback_err:
                logger.warning(
                    f"Could not rollback (connection may be closed): {rollback_err}"
                )
        raise
    finally:
        # Only close if connection is still open
        if not conn.closed:
            conn.close()


def is_connection_alive(conn: psycopg2.extensions.connection) -> bool:
    """Check if a database connection is still alive and usable.

    Args:
        conn: A psycopg2 database connection

    Returns:
        True if connection is open and responsive, False otherwise
    """
    if conn.closed:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


@contextmanager
def get_resilient_conn(
    db: str = "aws_rds",
    options_dict: dict | None = None,
    cursor_factory=None,
    max_retries: int = 3,
    retry_delay_base: float = 1.0,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Get a resilient database connection with automatic retry on connection failures.

    Use this for long-running jobs that may experience transient connection issues.
    Falls back to get_conn() for the actual connection creation.

    Args:
        db: Database identifier ('aws_rds', 'ijack', 'timescale')
        options_dict: Connection options (uses sensible defaults if None)
        cursor_factory: Cursor factory to use
        max_retries: Maximum number of connection retry attempts
        retry_delay_base: Base delay for exponential backoff (seconds)

    Yields:
        psycopg2.extensions.connection: Active database connection
    """
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            with get_conn(
                db=db, options_dict=options_dict, cursor_factory=cursor_factory
            ) as conn:
                # Verify connection is actually usable
                if not is_connection_alive(conn):
                    raise psycopg2.OperationalError("Connection health check failed")
                yield conn
                return  # Success, exit the retry loop

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last_exception = e
            error_msg = str(e).lower()

            # Check if this is a recoverable connection error
            is_connection_error = any(
                phrase in error_msg
                for phrase in [
                    "ssl connection has been closed",
                    "connection already closed",
                    "server closed the connection",
                    "connection refused",
                    "could not connect",
                    "connection timed out",
                    "connection health check failed",
                ]
            )

            if is_connection_error and attempt < max_retries:
                # Exponential backoff with jitter
                delay = retry_delay_base * (2**attempt) + random.uniform(
                    0, retry_delay_base
                )
                logger.warning(
                    f"Connection error (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                # Not recoverable or max retries exceeded
                raise

    # Should not reach here, but just in case
    if last_exception:
        raise last_exception


def _execute_queries(
    conn,
    cursor_factory,
    sql_commands_list: list,
    copy_expert_kwargs: dict | None,
    data: dict | tuple | None,
    log_query: bool,
    commit: bool,
    raise_error: bool,
    fetchall: bool,
) -> Tuple[list, list]:
    """Execute SQL queries on a connection (DRY helper function)

    This function contains the core query execution logic that was previously
    duplicated in run_query(). It's extracted to follow DRY principles.
    """
    columns, rows = [], []

    with conn.cursor(cursor_factory=cursor_factory) as cursor:
        for sql_command in sql_commands_list:
            try:
                if copy_expert_kwargs:
                    # Insert data into the table using the COPY command
                    if log_query:
                        try:
                            sql_string = copy_expert_kwargs.get(
                                "sql", "No SQL found"
                            ).as_string(cursor)
                        except AttributeError:
                            sql_string = copy_expert_kwargs.get("sql", "No SQL found")
                        logger.info(
                            f"Running PostgreSQL COPY EXPERT command with query: '{sql_string}'"
                        )
                    cursor.copy_expert(**copy_expert_kwargs)
                elif sql_command:
                    if log_query:
                        logger.info(f"Running query now... SQL to run: {sql_command}")
                    cursor.execute(sql_command, data)
            except psycopg2.Error as err:
                logger.info(f"ERROR executing SQL: '{sql_command}'\n\n Error: {err}")
                if raise_error:
                    raise
                conn.rollback()
            else:
                if commit:
                    conn.commit()
                if fetchall:
                    description = getattr(cursor, "description", None)
                    if not description:
                        logger.info("No data to fetch from cursor")
                    else:
                        columns = [str.lower(x[0]) for x in description]
                        rows: list = cursor.fetchall()

    return columns, rows


def run_query(
    sql: str = None,
    db: str = "aws_rds",
    fetchall: bool = True,
    commit: bool = False,
    raise_error: bool = True,
    data: dict | tuple = None,
    log_query: bool = False,
    # For super-efficient bulk inserts
    copy_expert_kwargs: dict = None,
    options_dict: dict = None,
    cursor_factory: int = None,
    isolation_level: int | None = None,
    sql_commands_list: list = None,
    conn=None,  # Optional connection to reuse
) -> Tuple[list, list]:
    """Run the SQL query and return the results as a tuple of columns and rows

    Args:
        conn: Optional database connection to reuse. If None, creates a new connection.
              This allows connection reuse across multiple queries for better performance.
    """

    # Initialize the variables
    columns, rows = [], []
    options_dict = options_dict or {}
    cursor_factory = cursor_factory or RealDictCursor

    if not sql_commands_list:
        sql_commands_list = [sql]

    time_start = time.time()

    # Use provided connection or create a new one
    if conn is not None:
        # Reuse existing connection (no context manager)
        if isolation_level is not None:
            conn.set_isolation_level(isolation_level)

        columns, rows = _execute_queries(
            conn=conn,
            cursor_factory=cursor_factory,
            sql_commands_list=sql_commands_list,
            copy_expert_kwargs=copy_expert_kwargs,
            data=data,
            log_query=log_query,
            commit=commit,
            raise_error=raise_error,
            fetchall=fetchall,
        )
    else:
        # Create new connection (original behavior for backward compatibility)
        with get_conn(db=db, cursor_factory=cursor_factory) as conn:
            if isolation_level is not None:
                conn.set_isolation_level(isolation_level)

            columns, rows = _execute_queries(
                conn=conn,
                cursor_factory=cursor_factory,
                sql_commands_list=sql_commands_list,
                copy_expert_kwargs=copy_expert_kwargs,
                data=data,
                log_query=log_query,
                commit=commit,
                raise_error=raise_error,
                fetchall=fetchall,
            )

    time_finish = time.time()
    execution_time = round(time_finish - time_start, 1)
    if execution_time > 1:
        logger.info(f"Time to execute query: {execution_time} seconds")

    return columns, rows


def send_twilio_sms(c, sms_phone_list, body) -> MessageInstance:
    """Send SMS messages with Twilio from +13067003245 or +13069884140"""
    message = MagicMock(spec=MessageInstance)
    if c.TEST_FUNC:
        return message

    # The Twilio character limit for SMS is 1,600
    unsubscribe_text = "\n\nReply STOP to unsubscribe from ALL IJACK SMS alerts."
    twilio_character_limit_sms = 1600 - len(unsubscribe_text)
    if len(body) > twilio_character_limit_sms:
        body = body[: (twilio_character_limit_sms - 3)] + "..."

    # Add this to every SMS alert, for compliance
    body += unsubscribe_text

    twilio_client = Client(
        os.environ["TWILIO_ACCOUNT_SID"], os.environ["TWILIO_AUTH_TOKEN"]
    )
    for phone_num in sms_phone_list:
        message: MessageInstance = twilio_client.messages.create(
            to=phone_num,
            # from_="+13067003245",
            from_="+13069884140",  # new number Apr 20, 2021
            body=body,
        )
        logger.info(f"SMS sent to {phone_num}")

    return message


def send_twilio_phone(c, phone_list, body):
    """Send phone call with Twilio from +13067003245 or +13069884140"""
    call = ""
    if c.TEST_FUNC:
        return call

    # Add this to every SMS alert, for compliance
    unsubscribe_text = "\n\nReply STOP to unsubscribe from ALL IJACK phone call alerts."
    body += unsubscribe_text

    twilio_client = Client(
        os.environ["TWILIO_ACCOUNT_SID"], os.environ["TWILIO_AUTH_TOKEN"]
    )
    for phone_num in phone_list:
        call = twilio_client.calls.create(
            to=phone_num,
            # from_="+13067003245",
            from_="+13069884140",  # new number Apr 20, 2021
            twiml=f"<Response><Say>Hello. The {body}</Say></Response>",
            # url=twiml_instructions_url
        )
        logger.info(f"Phone call sent to {phone_num}")

    return call


def send_mailgun_email(
    c, text="", html="", emailees_list=None, subject="IJACK Alert", images=None
) -> requests.models.Response:
    """Send email using Mailgun"""
    # Initialize the return code
    rc = ""
    if c.TEST_FUNC:
        return rc

    if emailees_list is None:
        emailees_list = ["smccarthy@myijack.com"]

    # If html is included, use that. Otherwise use text
    if html == "":
        key = "text"
        value = text
    else:
        key = "html"
        value = html

    # Add inline attachments, if any
    images2 = images
    if images is not None:
        images2 = []
        for item in images:
            images2.append(("inline", open(item, "rb")))

    # if c.DEV_TEST_PRD in ['testing', 'production']:
    # logger.debug(f"c.DEV_TEST_PRD: {c.DEV_TEST_PRD}")
    if len(emailees_list) > 0:
        rc = requests.post(
            "https://api.mailgun.net/v3/myijack.com/messages",
            auth=("api", os.environ["MAILGUN_API_KEY"]),
            files=images2,
            data={
                "h:sender": "no_reply@myijack.com",
                "from": "no_reply@myijack.com",
                "to": emailees_list,
                "subject": subject,
                key: value,
            },
        )
        logger.info(
            f"Email sent to emailees_list: '{str(emailees_list)}' \nSubject: {subject} \nrc.status_code: {rc.status_code}"
        )
        assert rc.status_code == 200

    return rc


def get_aws_iot_ats_endpoint():
    """
    Get the "Data-ATS" endpoint instead of the
    untrusted "Symantec" endpoint that's built-in.
    """
    # iot_client = boto3.client("iot", "us-west-2", verify=True)
    # details = iot_client.describe_endpoint(endpointType="iot:Data-ATS")
    # host = details.get("endpointAddress")
    # url = f"https://{host}"
    url = "https://a2zzb3nqu1iqym-ats.iot.us-west-2.amazonaws.com"
    return url


def get_client_iot() -> boto3.client:
    """Get the AWS IoT boto3 client"""
    client_name = "iot-data"
    if client_name == "iot-data":
        # Need this to avoid "CERTIFICATE_VERIFY_FAILED" error
        endpoint_url = get_aws_iot_ats_endpoint()
    else:
        # Auto-constructed
        endpoint_url = None

    client_iot = boto3.client(
        client_name,
        region_name="us-west-2",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
        # it still uses SSL even if verification is turned off
        use_ssl=True,
        verify=True,
        endpoint_url=endpoint_url,
    )
    # Change the botocore logger from logging.DEBUG to INFO,
    # since DEBUG produces too many messages
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

    return client_iot


def get_iot_device_shadow(client_iot, aws_thing):
    """This function gets the current thing state"""

    response_payload = {}
    try:
        response = client_iot.get_thing_shadow(thingName=aws_thing)
        streamingBody = response["payload"]
        response_payload = json.loads(streamingBody.read())
    except Exception:
        logger.exception(
            f"ERROR! Probably no shadow exists for aws_thing '{aws_thing}'..."
        )
    else:
        response_payload["aws_thing"] = aws_thing

    return response_payload


def subprocess_run(command_list, method="run", shell=False, sleep=0, log_results=False):
    """
    Run the subprocess.run() command and print the output to the log
    Return the return code (rc) and standard output (stdout)
    """

    rc = 1
    stdout = ""
    try:
        if method == "run":
            cp = subprocess.run(
                command_list,
                shell=shell,
                stdout=PIPE,
                stderr=STDOUT,
                universal_newlines=True,
            )
            rc = cp.returncode
            stdout = cp.stdout

        elif method == "Popen":
            _ = subprocess.Popen(command_list)
            rc = 0
            stdout = ""

        elif method == "check_output":
            cp = subprocess.check_output(
                command_list, shell=shell, universal_newlines=True
            )
            rc = "N/A"
            stdout = cp
    except Exception:
        logger.exception(
            f"ERROR running command with subprocess_run(): '{command_list}'"
        )

    else:
        log_msg = f"rc: '{rc}' from command: \n'{command_list}'. \nstdout/stderr: \n{stdout}\n\n"
        if log_results:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)

        # Sleep for 'sleep' seconds, if desired, after completing the process
        time.sleep(sleep)

    return rc, stdout


def find_pids(search_string: str) -> List:
    """Find the PID of the running process based on the search string, and return a list of PIDs"""
    rc, stdout = subprocess_run(["/usr/bin/pgrep", "-f", search_string])
    list_of_pids = []
    if rc == 0:
        for line in stdout.splitlines():
            stripped = line.rstrip("\r\n")
            list_of_pids.append(stripped)

    return list_of_pids


def exit_if_already_running(c: Config, filename: str) -> None:
    """If this program is already running, exit"""
    list_of_pids = find_pids(filename)
    if len(list_of_pids) > 1:
        logger.warning(
            f"This scheduled process is already running with PID(s) of '{list_of_pids}'. Exiting now to avoid overloading the system."
        )
        if not c.TEST_FUNC:
            sys.exit(0)


def kill_pids(list_of_pids) -> List:
    """Kill all process IDs (PIDs) in the list_of_pids"""
    assert isinstance(list_of_pids, list)

    list_of_pids_killed = []
    for pid in list_of_pids:
        try:
            int_pid = int(pid)
            os.kill(int_pid, signal.SIGTERM)
            logger.info(f"PID {pid} killed")
            list_of_pids_killed.append(pid)
        except Exception:
            logger.exception(
                f"PID {pid} cannot be cast to an integer for os.kill(), which requires an integer"
            )

    return list_of_pids_killed


def check_if_c_in_args(args) -> Config:
    """Check if the 'utils.Config object' is in the args, and return it"""
    c = None
    for arg in args:
        if "utils.Config object" in str(arg):
            c = arg
            break
    if c is None:
        c = Config()
    return c


def is_time_between(
    begin_time: datetime, end_time: datetime, check_time: datetime = None
) -> bool:
    """Checks if the 'check_time' is between the begin_time and end_time"""

    # If check time is not given, default to current UTC time
    check_time = check_time or utcnow_naive().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time

    # crosses midnight
    return check_time >= begin_time or check_time <= end_time


def send_error_messages(
    c: Config | None = None,
    err: Exception | None = None,
    filename: Path | None = None,
    want_email: bool = True,
    want_sms: bool = False,
) -> None:
    """Send error messages to email and/or SMS"""

    if not isinstance(c, Config):
        c = Config()

    # Every morning at 9:01 UTC I get an email that says "server closed the connection unexpectedly.
    # This probably means the server terminated abnormally before or while processing the request."
    check_dt: datetime = utcnow_naive()
    check_dt_sk_time: datetime = utcnow_aware().astimezone(
        pytz.timezone("America/Regina")
    )
    logger.info(f"The time of the error is {check_dt_sk_time} SK time")
    try:
        if is_time_between(
            begin_time=dt_time(hour=9, minute=0),
            end_time=dt_time(hour=9, minute=3),
            check_time=check_dt.time(),
        ) and "server closed the connection" in str(err):
            # Don't send an email if it's the morning and the error is about the server closing the connection
            return None
    except Exception as err_inner:
        logger.exception(
            f"ERROR checking the time of the error... \nError msg: {err_inner}"
        )

    logger.error(f"ERROR running program! Closing now... \nError msg: {err}")
    alertees_email = ["smccarthy@myijack.com"]
    alertees_sms = ["+14036897250"]
    subject = f"{filename} ERROR!"
    msg_sms = f"Sean, check 'postgresql_scheduler' module '{filename}' now! There has been an error at {check_dt_sk_time} SK time!"
    msg_email = (
        msg_sms
        + f"\n\nError type: {type(err).__name__}. Class: {err.__class__.__name__}. \n\nArgs: {err.args}. \n\nError message: {err}"
    )
    msg_email += f"\n\nTraceback: {traceback.format_exc()}"

    message: str = ""
    if want_sms:
        message = send_twilio_sms(c, alertees_sms, msg_sms)

    rc: requests.models.Response | None = None
    if want_email:
        rc = send_mailgun_email(
            c,
            text=msg_email,
            html="",
            emailees_list=alertees_email,
            subject=subject,
        )

    c.TEST_DICT["message"] = message
    c.TEST_DICT["rc"] = rc
    c.TEST_DICT["msg_sms"] = msg_sms

    return None


def error_wrapper(filename: str):
    def wrapper_outer(func):
        @functools.wraps(func)
        def wrapper_inner(*args, **kwargs):
            # Need to make this in the outer scope first and overwrite it if necessary...
            c = None
            try:
                # Do something before
                c = check_if_c_in_args(args)
                value = None

                # # If we're testing the alerting (when an error happens), raise an exception
                # if c.TEST_ERROR:
                #     raise ValueError

                # Run the actual function
                value = func(*args, **kwargs)

            # Do something after
            except Exception as err:
                # Send error messages to email and/or SMS
                filename2 = filename or Path(__file__).name
                send_error_messages(c, err, filename2, want_email=True, want_sms=True)

                raise

            return value

        return wrapper_inner

    return wrapper_outer


def utc_to_local_dt(dt_utc, to_pytz_timezone=pytz.timezone("America/Regina")):
    """
    Takes a non-timezone-aware UTC datetime() in structured
    (non-string) format and converts it to the pytz_timezone wanted
    [e.g. pytz.timezone('America/Edmonton')],
    still in datetime() format
    """
    return dt_utc.replace(tzinfo=pytz.utc).astimezone(to_pytz_timezone)


def utc_datetime_to_string(
    dt_utc,
    to_pytz_timezone=pytz.timezone("America/Regina"),
    format_string="%Y-%m-%d %H:%M:%S %Z%z",
):
    """
    Takes a UTC datetime() in structured (non-string) format
    and converts it to a printable string, with the format specified.
    """
    return utc_to_local_dt(dt_utc, to_pytz_timezone).strftime(format_string)


def utc_timestamp_to_datetime_string(
    timestamp_utc,
    to_pytz_timezone=pytz.timezone("America/Regina"),
    format_string="%Y-%m-%d %H:%M:%S %Z%z",
):
    """
    Takes a UTC timestamp and converts it to a printable string,
    with the format specified.
    """
    dt = datetime.fromtimestamp(timestamp_utc)
    return utc_datetime_to_string(dt, to_pytz_timezone, format_string)


def seconds_since_last_any_msg(shadow) -> Tuple[float, str, str]:
    """How many seconds has it been since we received ANY message from the gateway at AWS?"""

    time_received_latest = 0
    key_latest = None
    meta_reported = shadow.get("metadata", {}).get("reported", {})
    for key, _ in meta_reported.items():
        # metadata contains the timestamps for each attribute in the desired and reported sections so that you can determine when the state was updated
        if (
            "wait_okay" in key  # alerts sent flags
            # or key == 'connected' # AWS Lambda updates this for the last will and testament
            or key.startswith(
                "AWS_"
            )  # commands from AWS are not okay since it includes the desired state...
            or key.startswith("C__")  # config data, which is refreshed periodically
            # Latitude and longitude can be updated by the website itself, if the unit is selected!
            or key == "LATITUDE"
            or key == "LONGITUDE"
            or key == "connected"
        ):
            continue

        meta_reported_sub_dict = meta_reported.get(key, {})
        if not isinstance(meta_reported_sub_dict, dict):
            continue

        time_received = meta_reported_sub_dict.get("timestamp", 0)
        # Ensure time_received is a numeric type (int or float), not a dict or other type
        if not isinstance(time_received, (int, float)):
            continue

        if time_received > time_received_latest:
            time_received_latest = time_received
            key_latest = key

            # ####################
            # # For debugging only
            # # Convert to a datetime
            # since_when_time_received = datetime.utcfromtimestamp(time_received_latest)
            # # Get the timedelta since we started waiting
            # time_delta_time_received = (utcnow_naive() - since_when_time_received)
            # # How many seconds has it been since we started waiting?
            # seconds_elapsed_total = time_delta_time_received.days*24*60*60 + time_delta_time_received.seconds
            # current_app.logger.debug(f"Most recent metric in AWS IoT device shadow: {key_latest} as of {round(seconds_elapsed_total/60, 1)} minutes ago")
            # current_app.logger.debug("")

    # How many seconds has it been since we started waiting?
    seconds_elapsed_total = round(time.time() - time_received_latest, 1)

    mins_ago = round(seconds_elapsed_total / 60, 1)
    hours_ago = round(mins_ago / 60, 1)
    days_ago = round(hours_ago / 24, 1)
    if seconds_elapsed_total < 60:
        msg = f"{seconds_elapsed_total} seconds"
        # color_time_since = "success"
    elif mins_ago < 6:
        msg = f"{mins_ago} minutes"
        # color_time_since = "success"
    elif mins_ago < 16:
        msg = f"{mins_ago} minutes"
        # color_time_since = "warning"
    elif mins_ago < 60:
        msg = f"{mins_ago} minutes"
        # color_time_since = "danger"
    elif hours_ago < 24:
        msg = f"{hours_ago} hours"
        # color_time_since = "danger"
    else:
        msg = f"{days_ago} days"
        # color_time_since = "danger"

    logger.info(
        "Most recent metric in AWS IoT device shadow: %s as of %s ago",
        key_latest,
        msg,
    )

    return seconds_elapsed_total, msg, key_latest


def get_power_units_and_unit_types() -> dict:
    """Get the power units and unit types mapping"""
    sql = """
    SELECT
        id as structure_id,
        power_unit_id,
        power_unit_str,
        power_unit_type,
        gateway_id,
        aws_thing,
        unit_type_id,
        unit_type,
        case when unit_type_id in (1, 4) then false else true end as is_egas_type,
        model_type_id,
        model,
        model_unit_type_id,
        model_unit_type,
        customer_id,
        customer
    FROM public.vw_structures_joined
        --structure must have a power unit
        where power_unit_id is not null
            --power unit must have a gateway or there's no data
            and gateway_id is not null
    """
    columns, rows = run_query(
        sql, db="ijack", fetchall=True, raise_error=True, log_query=False
    )
    df = pd.DataFrame(rows, columns=columns)
    dict_ = dict(zip(df["power_unit_str"], df["is_egas_type"]))
    return dict_
