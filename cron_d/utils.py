import functools
import json
import logging
import os
import pathlib
import platform
import signal
import subprocess
import sys
import time
import datetime
from logging.handlers import TimedRotatingFileHandler
from subprocess import PIPE, STDOUT
import traceback
from typing import Tuple, List

import boto3
import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from twilio.rest import Client
import pytz


LOG_LEVEL = logging.INFO
# logger = logging.getLogger(__name__)


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
    ]
    # For returning values in the "c" config object
    TEST_DICT = {}


def configure_logging(name, logfile_name, path_to_log_directory="/var/log/"):
    """Configure logger"""
    global LOG_LEVEL

    logger = logging.getLogger(name)
    # Override the default logging.WARNING level so all messages can get through to the handlers
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s : %(module)s : %(lineno)d : %(levelname)s : %(funcName)s : %(message)s"
    )

    # date_for_log_filename = datetime.datetime.now().strftime('%Y-%m-%d')
    # log_filename = f"{date_for_log_filename}_{logfile_name}.log"
    log_filename = f"{logfile_name}.log"
    log_filepath = os.path.join(path_to_log_directory, log_filename)

    if platform.system() == "Linux":
        # fh = logging.FileHandler(filename=log_filepath)
        fh = TimedRotatingFileHandler(
            filename=log_filepath,
            when="H",
            interval=1,
            backupCount=48,
            encoding=None,
            delay=False,
            utc=False,
            atTime=None,
        )
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


def get_conn(c, db="ijack", cursor_factory=None):
    """Get connection to IJACK database"""

    if db == "ijack":
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

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=5,
        cursor_factory=cursor_factory,
        keepalives=1,  # is default
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    return conn


def run_query(
    c,
    sql,
    db="ijack",
    fetchall=False,
    commit=False,
    conn=None,
    execute=True,
    raise_error: bool = False,
    values_dict: dict = None,
) -> Tuple[List, List]:
    """Run and time the SQL query"""

    is_close_conn = False
    if conn is None:
        conn = get_conn(c, db)
        is_close_conn = True

    columns = None
    rows = None

    # with conn.cursor(cursor_factory=DictCursor) as cursor:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        c.logger.info("Running query now... SQL to run: \n%s", sql)
        time_start = time.time()
        if execute:
            try:
                cursor.execute(sql, values_dict)
            except Exception:
                c.logger.exception(f"ERROR executing SQL: '{sql}'")
                if raise_error:
                    raise
            else:
                if commit:
                    conn.commit()
                if fetchall:
                    columns = [str.lower(x[0]) for x in cursor.description]
                    rows = cursor.fetchall()

    time_finish = time.time()
    c.logger.info(f"Time to execute query: {round(time_finish - time_start)} seconds")

    if is_close_conn:
        conn.close()
        del conn

    return columns, rows


def error_wrapper_old(c, func, *args, **kwargs):
    """So the loop can continue even if a function fails"""

    try:
        func(*args, **kwargs)
    except Exception:
        c.logger.exception(f"Problem running function: {func}")
        # Keep going regardless

    return None


def send_twilio_sms(c, sms_phone_list, body):
    """Send SMS messages with Twilio from +13067003245 or +13069884140"""
    message = ""
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
        message = twilio_client.messages.create(
            to=phone_num,
            # from_="+13067003245",
            from_="+13069884140",  # new number Apr 20, 2021
            body=body,
        )
        c.logger.info(f"SMS sent to {phone_num}")

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
            twiml=f"<Response><Say>Hello. The {body}</Say></Response>"
            # url=twiml_instructions_url
        )
        c.logger.info(f"Phone call sent to {phone_num}")

    return call


def send_mailgun_email(
    c, text="", html="", emailees_list=None, subject="IJACK Alert", images=None
):
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
    # c.logger.debug(f"c.DEV_TEST_PRD: {c.DEV_TEST_PRD}")
    if len(emailees_list) > 0:
        rc = requests.post(
            "https://api.mailgun.net/v3/myijack.com/messages",
            auth=("api", os.environ["MAILGUN_API_KEY"]),
            files=images2,
            data={
                "h:sender": "alerts@myijack.com",
                "from": "alerts@myijack.com",
                "to": emailees_list,
                "subject": subject,
                key: value,
            },
        )
        c.logger.info(
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


def get_iot_device_shadow(c, client_iot, aws_thing):
    """This function gets the current thing state"""

    response_payload = {}
    try:
        response = client_iot.get_thing_shadow(thingName=aws_thing)
        streamingBody = response["payload"]
        response_payload = json.loads(streamingBody.read())
    except Exception:
        c.logger.exception(
            f"ERROR! Probably no shadow exists for aws_thing '{aws_thing}'..."
        )
    else:
        response_payload["aws_thing"] = aws_thing

    return response_payload


def subprocess_run(
    c, command_list, method="run", shell=False, sleep=0, log_results=False
):
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
        c.logger.exception(
            f"ERROR running command with subprocess_run(): '{command_list}'"
        )

    else:
        log_msg = f"rc: '{rc}' from command: \n'{command_list}'. \nstdout/stderr: \n{stdout}\n\n"
        if log_results:
            c.logger.info(log_msg)
        else:
            c.logger.debug(log_msg)

        # Sleep for 'sleep' seconds, if desired, after completing the process
        time.sleep(sleep)

    return rc, stdout


def find_pids(c, search_string):
    """Find the PID of the running process based on the search string, and return a list of PIDs"""
    rc, stdout = subprocess_run(c, ["/usr/bin/pgrep", "-f", search_string])
    list_of_pids = []
    if rc == 0:
        for line in stdout.splitlines():
            stripped = line.rstrip("\r\n")
            list_of_pids.append(stripped)

    return list_of_pids


def exit_if_already_running(c, filename):
    """If this program is already running, exit"""
    list_of_pids = find_pids(c, filename)
    if len(list_of_pids) > 1:
        c.logger.warning(
            f"This scheduled process is already running with PID(s) of '{list_of_pids}'. Exiting now to avoid overloading the system."
        )
        if not c.TEST_FUNC:
            sys.exit(0)


def kill_pids(c, list_of_pids):
    """Kill all process IDs (PIDs) in the list_of_pids"""
    assert isinstance(list_of_pids, list)

    list_of_pids_killed = []
    for pid in list_of_pids:
        try:
            int_pid = int(pid)
        except Exception:
            c.logger.exception(
                f"PID {pid} cannot be cast to an integer for os.kill(), which requires an integer"
            )

        os.kill(int_pid, signal.SIGTERM)
        c.logger.info(f"PID {pid} killed")
        list_of_pids_killed.append(pid)

    return list_of_pids_killed


# def error_wrapper(func):

# 	@functools.wraps(func)
# 	def wrapper_decorator(*args, **kwargs):
# 		# Do something before
# 		value = func(*args, **kwargs)
# 		# Do something after
# 		return value

#     return wrapper_decorator


def check_if_c_in_args(args):
    c = None
    for arg in args:
        if "utils.Config object" in str(arg):
            c = arg
            break
    if c is None:
        c = Config()
        c.logger = configure_logging(
            __name__,
            logfile_name=pathlib.Path(__file__).stem,
            path_to_log_directory="/var/log/",
        )
    return c


def is_time_between(begin_time, end_time, check_time=None):
    """Checks if the 'check_time' is between the begin_time and end_time"""
    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.datetime.utcnow().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else:  # crosses midnight
        return check_time >= begin_time or check_time <= end_time


def error_wrapper():
    def wrapper_outer(func):
        @functools.wraps(func)
        def wrapper_inner(*args, **kwargs):
            # Need to make this in the outer scope first and overwrite it if necessary...
            c = Config()
            c.logger = configure_logging(
                __name__,
                logfile_name=pathlib.Path(__file__).stem,
                path_to_log_directory="/var/log/",
            )
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
                # Every morning at 9:01 UTC I get an email that says "server closed the connection unexpectedly.
                # This probably means the server terminated abnormally before or while processing the request."
                check_dt = datetime.datetime.utcnow()
                c.logger.info(f"The time of the error is {check_dt}")
                try:
                    if (
                        is_time_between(
                            begin_time=datetime.time(hour=9, minute=0),
                            end_time=datetime.time(hour=9, minute=3),
                            check_time=check_dt.time(),
                        )
                        and "server closed the connection" in str(err)
                    ):
                        return None
                except Exception as err_inner:
                    c.logger.exception(
                        f"ERROR checking the time of the error... \nError msg: {err_inner}"
                    )
                    err += f"\n\nWhile processing the initial error, another error happened while checking the time of the error: \n\n{err_inner}"

                filename = pathlib.Path(__file__).name
                c.logger.exception(
                    f"ERROR running program! Closing now... \nError msg: {err}"
                )
                alertees_email = ["smccarthy@myijack.com"]
                alertees_sms = ["+14036897250"]
                subject = f"IJACK {filename} ERROR!!!"
                msg_sms = f"Sean, check 'postgresql_scheduler' module '{filename}' now! There has been an error at {check_dt} UTC time!"
                msg_email = (
                    msg_sms
                    + f"\n\nError type: {type(err).__name__}. Class: {err.__class__.__name__}. \n\nArgs: {err.args}. \n\nError message: {err}"
                )
                msg_email += f"\n\nTraceback: {traceback.format_exc()}"

                message = send_twilio_sms(c, alertees_sms, msg_sms)
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

                raise

            return value

        return wrapper_inner

    return wrapper_outer


def get_all_gateways(c) -> list:
    """Get all gateways from database"""

    # These are all the metrics that will be put in the AWS IoT device shadow as "C__{METRIC}"
    SQL = """
        select aws_thing, gateway, customer, mqtt_topic, cust_sub_group_abbrev,
            unit_type, apn, 
            location, power_unit, model, 
            time_zone,
            heartbeat_enabled, online_hb_enabled, spm, stboxf, suction, discharge, hyd_temp, 
            wait_time_mins, wait_time_mins_ol, wait_time_mins_spm, wait_time_mins_stboxf, 
            wait_time_mins_suction, wait_time_mins_discharge, wait_time_mins_hyd_temp,
            hyd_oil_lvl_thresh, hyd_filt_life_thresh, hyd_oil_life_thresh,
            wait_time_mins_hyd_oil_lvl, wait_time_mins_hyd_filt_life, wait_time_mins_hyd_oil_life,
            ip_modbus
        from public.gateways
        where aws_thing <> 'test'
            and aws_thing is not null
            and customer_id != 21 -- demo customer
    """
    _, rows = run_query(c, SQL, db="ijack", fetchall=True)

    return rows


def utc_to_local_dt(dt_utc, to_pytz_timezone=pytz.timezone("America/Regina")):
    """
    Takes a non-timezone-aware UTC datetime.datetime() in structured
    (non-string) format and converts it to the pytz_timezone wanted
    [e.g. pytz.timezone('America/Edmonton')],
    still in datetime.datetime() format
    """
    return dt_utc.replace(tzinfo=pytz.utc).astimezone(to_pytz_timezone)


def utc_datetime_to_string(
    dt_utc,
    to_pytz_timezone=pytz.timezone("America/Regina"),
    format_string="%Y-%m-%d %H:%M:%S %Z%z",
):
    """
    Takes a UTC datetime.datetime() in structured (non-string) format
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
    dt = datetime.datetime.fromtimestamp(timestamp_utc)
    return utc_datetime_to_string(dt, to_pytz_timezone, format_string)


def seconds_since_last_any_msg(c, shadow):
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
        ):
            continue

        meta_reported_sub_dict = meta_reported.get(key, {})
        if not isinstance(meta_reported_sub_dict, dict):
            continue

        time_received = meta_reported_sub_dict.get("timestamp", 0)
        if time_received > time_received_latest:
            time_received_latest = time_received
            key_latest = key

            # ####################
            # # For debugging only
            # # Convert to a datetime
            # since_when_time_received = datetime.utcfromtimestamp(time_received_latest)
            # # Get the timedelta since we started waiting
            # time_delta_time_received = (datetime.utcnow() - since_when_time_received)
            # # How many seconds has it been since we started waiting?
            # seconds_elapsed_total = time_delta_time_received.days*24*60*60 + time_delta_time_received.seconds
            # current_app.logger.debug(f"Most recent metric in AWS IoT device shadow: {key_latest} as of {round(seconds_elapsed_total/60, 1)} minutes ago")
            # current_app.logger.debug("")

    # How many seconds has it been since we started waiting?
    seconds_elapsed_total = round(time.time() - time_received_latest, 1)

    c.logger.debug(
        "Most recent metric in AWS IoT device shadow: %s as of %s minutes ago",
        key_latest,
        round(seconds_elapsed_total / 60, 1),
    )

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

    return seconds_elapsed_total, msg
