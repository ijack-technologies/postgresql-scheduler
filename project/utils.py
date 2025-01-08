import functools
import json
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from datetime import time as dt_time
from pathlib import Path
from subprocess import PIPE, STDOUT
from typing import List, Tuple
from unittest.mock import MagicMock
import boto3
import pandas as pd
import psycopg2
import pytz
import requests
from psycopg2.extras import RealDictCursor
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


def get_conn(db="ijack", cursor_factory=None):
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
    sql: str = None,
    db: str = "ijack",
    fetchall: bool = False,
    commit: bool = False,
    conn=None,
    execute: bool = True,
    raise_error: bool = False,
    values_dict: dict = None,
    log_query: bool = True,
    copy_from_kwargs: dict = None,
    # No need to convert to list of dicts, since we're using RealDictCursor
    # as_list_of_dicts: bool = False,
) -> Tuple[List, List]:
    """Run and time the SQL query"""

    is_close_conn = False
    if conn is None:
        conn = get_conn(db)
        is_close_conn = True

    columns = None
    rows = None

    # with conn.cursor(cursor_factory=DictCursor) as cursor:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if log_query:
            logger.info("Running query now... SQL to run: \n%s", sql)
        time_start = time.time()
        if execute:
            try:
                if copy_from_kwargs:
                    cursor.copy_from(**copy_from_kwargs)
                else:
                    cursor.execute(sql, values_dict)
            except Exception as err:
                logger.error(f"ERROR executing SQL: '{sql}'\n\n Error: {err}")
                if raise_error:
                    raise
            else:
                if commit:
                    conn.commit()
                if fetchall:
                    columns = [str.lower(x[0]) for x in cursor.description]
                    rows: list = cursor.fetchall()
                    # No need to convert to list of dicts, since we're using RealDictCursor
                    # if as_list_of_dicts:
                    #     rows: list = get_list_of_dicts(columns, rows)

    time_finish = time.time()
    execution_time = round(time_finish - time_start, 1)
    if execution_time > 10:
        logger.info(f"Time to execute query: {execution_time} seconds")

    if is_close_conn:
        conn.close()
        del conn

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
                "h:sender": "alerts@myijack.com",
                "from": "alerts@myijack.com",
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


def get_iot_device_shadow(c, client_iot, aws_thing):
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


def find_pids(c, search_string):
    """Find the PID of the running process based on the search string, and return a list of PIDs"""
    rc, stdout = subprocess_run(c, ["/usr/bin/pgrep", "-f", search_string])
    list_of_pids = []
    if rc == 0:
        for line in stdout.splitlines():
            stripped = line.rstrip("\r\n")
            list_of_pids.append(stripped)

    return list_of_pids


def exit_if_already_running(c, filename) -> None:
    """If this program is already running, exit"""
    list_of_pids = find_pids(c, filename)
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


def get_all_power_units_config_metrics(c) -> list:
    """
    Get all power units from database, and all the fields we're going
    to update in the AWS IoT device shadow with C__{METRIC}
    """

    # These are all the metrics that will be put in the AWS IoT device shadow as "C__{METRIC}"
    SQL = """
        with modbus as (           
            select 
				power_unit_id,
                STRING_AGG(
                    CONCAT(ip_address, '>', subnet, '>', gateway),
                    ','
                ) as modbus_networks
            from power_units_modbus_networks
            group by power_unit_id
        )
        select gw.aws_thing, 
            gw.gateway, 
            cust.customer, 
            cust.mqtt_topic, 
            cust_sub.abbrev AS cust_sub_group_abbrev,
            ut.unit_type, 
            pu.apn,
            CASE WHEN str.downhole IS NULL OR str.downhole = ''::text THEN str.surface
                ELSE (str.downhole || ' @ '::text) || str.surface
            END AS location, 
            pu.power_unit, 
            mt.model,
            tz.time_zone,
            -- Alert settings
            pu.wait_time_mins,
            pu.wait_time_mins_ol,
            pu.wait_time_mins_suction,
            pu.wait_time_mins_discharge,
            pu.wait_time_mins_spm,
            pu.wait_time_mins_stboxf,
            pu.wait_time_mins_hyd_temp,
            pu.hyd_oil_lvl_thresh,
            pu.hyd_filt_life_thresh,
            pu.hyd_oil_life_thresh,
            pu.wait_time_mins_hyd_oil_lvl,
            pu.wait_time_mins_hyd_filt_life,
            pu.wait_time_mins_hyd_oil_life,
            pu.wait_time_mins_chk_mtr_ovld,
            pu.wait_time_mins_pwr_fail,
            pu.wait_time_mins_soft_start_err,
            pu.wait_time_mins_grey_wire_err,
            pu.wait_time_mins_ae011,
            pu.heartbeat_enabled,
            pu.online_hb_enabled,
            pu.suction,
            pu.discharge,
            pu.spm,
            pu.stboxf,
            pu.hyd_temp,
            --pu.ip_modbus,
            --pu.subnet_modbus,
            --pu.gateway_modbus,
            --modbus.ip_address as ip_modbus,
            --modbus.subnet as subnet_modbus,
            --modbus.gateway as gateway_modbus
            modbus.modbus_networks
        FROM gw gw
        LEFT JOIN power_units pu ON gw.power_unit_id = pu.id
        LEFT JOIN structures str ON pu.id = str.power_unit_id
        LEFT JOIN myijack.structure_customer_rel str_cust_rel ON str_cust_rel.structure_id = str.id
        LEFT JOIN myijack.customers cust ON str_cust_rel.customer_id = cust.id
        LEFT JOIN myijack.cust_sub_groups cust_sub ON cust_sub.id = str.cust_sub_group_id
        LEFT JOIN myijack.time_zones tz ON str.time_zone_id = tz.id
        LEFT JOIN myijack.model_types mt ON str.model_type_id = mt.id
        LEFT JOIN myijack.unit_types ut ON str.unit_type_id = ut.id
        LEFT JOIN modbus ON pu.id = modbus.power_unit_id
        where gw.aws_thing <> 'test'
            and gw.aws_thing is not null
            and cust.id is distinct from 21 -- demo customer
    """
    _, rows = run_query(SQL, db="ijack", fetchall=True)
    if not rows:
        raise ValueError("No rows found in the database for the power units!!!")

    return rows


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


def seconds_since_last_any_msg(c, shadow) -> Tuple[float, str, str]:
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


def get_power_units_and_unit_types(conn=None) -> dict:
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
        sql, db="ijack", conn=conn, fetchall=True, raise_error=True, log_query=False
    )
    df = pd.DataFrame(rows, columns=columns)
    dict_ = dict(zip(df["power_unit_str"], df["is_egas_type"]))
    return dict_
