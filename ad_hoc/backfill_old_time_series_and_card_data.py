#######################################################################################################
# This module is meant to be run from development only, not from the production server/Docker container
# It updates the gps_lat and gps_lon fields in the public.structures table, using the API from legallandconverter.com
# API instructions here: https://legallandconverter.com/p51.html#OVERVIEW
# It costs USD $0.10 per lookup, so don't be wasteful since there are 500+ lookups ($50)
#######################################################################################################

import concurrent.futures
import json
import logging
import os
import pathlib
import platform
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv



def insert_path(pythonpath):
    """Insert pythonpath into the front of the PATH environment
    variable, before importing anything from canpy"""
    try:
        sys.path.index(str(pythonpath))
    except ValueError:
        sys.path.insert(0, str(pythonpath))


# For running in development only
project_folder = pathlib.Path(__file__).absolute().parent.parent
ad_hoc_folder = project_folder.joinpath("ad_hoc")
cron_d_folder = project_folder.joinpath("cron.d")
insert_path(cron_d_folder)  # second in path
insert_path(ad_hoc_folder)  # first in path

from utils import (
    error_wrapper,  # Config,; configure_logging,
    exit_if_already_running,
    get_conn,
    run_query,
)

load_dotenv()

# # Warning for the user, in case she started this program by accident. This is a chance to cancel.
# yes_or_no = input(
#     "Are you sure you want to backfill all time series data? \n(y)es or (n)o: "
# )
# y_or_n_lower = str(yes_or_no).lower()[0]
# if y_or_n_lower == "y":
#     print("Continuing...")
# elif y_or_n_lower == "n":
#     print("Exiting now!")
#     sys.exit()


# Create SQS client
sqs = boto3.client("sqs", region_name="us-west-2")

# sqs_queue_url = 'https://sqs.us-west-2.amazonaws.com/960752594355/test_sqs'
sqs_queue_url = "https://sqs.us-west-2.amazonaws.com/960752594355/timescale_all"


def configure_logging(
    name,
    path_to_log_directory="/var/log/",
    use_file_handler=True,
    log_level_fh=logging.INFO,
    log_level_sh=logging.INFO,
):
    """Configure logger"""

    os.makedirs(path_to_log_directory, exist_ok=True)
    logger = logging.getLogger(name)
    # Override the default logging.WARNING level so all messages can get through to the handlers
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d : %(message)s"
    )

    if platform.system() == "Linux" and use_file_handler:
        # fh = TimedRotatingFileHandler(
        #     filename=os.path.join(path_to_log_directory, f"{name}_inserter.log"),
        #     when="H",
        #     interval=1,
        #     backupCount=3,
        #     encoding=None,
        #     delay=False,
        #     utc=False,
        #     atTime=None,
        # )
        fh = RotatingFileHandler(
            os.path.join(path_to_log_directory, f"{name}_inserter.log"),
            maxBytes=1_024_000,
            backupCount=10,
        )
        fh.setLevel(log_level_fh)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # sh = logging.StreamHandler(sys.stdout)
    sh = logging.StreamHandler()
    sh.setLevel(log_level_sh)
    sh.setFormatter(formatter)
    # print(f"logger.handlers before adding streamHandler: {logger.handlers}")
    logger.addHandler(sh)
    # print(f"logger.handlers after adding streamHandler: {logger.handlers}")

    # # Test logger
    # sh.setLevel(logging.DEBUG)
    # logger.debug(f"Testing the logger: platform.system() = {platform.system()}")
    # sh.setLevel(log_level_sh)

    return logger


class Config:
    """Config class"""

    TEST_FUNC = False
    REAL_TIME_METRICS = (
        "CGP_RT",
        "DGP_RT",
        "HPE_RT",
        "HP_LIMIT",  # we like to see this with HPE_RT
        "SPM_EGAS_RT",
    )
    TIME_SERIES_METRICS = (
        "OHE",
        "OHU",
        "SHE",
        "SHU",
        "SPM",
        "SPM_EGAS",
        "CGP",
        "DGP",
        "DTP",
        "HPU",
        "HPE",
        "HT",
        "HT_EGAS",
        "AGFT",
        "AGFM",
        "AGFN",
        "MGP",
        "NGP",
        "E3M3_D",
        "M3PD",
        "HP_LIMIT",
        "MSP",
        # UNOGAS total horsepower
        "HPT",
        # UNO card metrics
        "MPRL_MAX",
        "MPRL_AVG",
        "MPRL_MIN",
        "PPRL_MAX",
        "PPRL_AVG",
        "PPRL_MIN",
        "AREA_MAX",
        "AREA_AVG",
        "AREA_MIN",
        "PF_MAX",
        "PF_AVG",
        "PF_MIN",
        # XFER new metrics v311
        "HP_RAISING_AVG",
        "HP_LOWERING_AVG",
        "DER_DTP_VPD",
        "DER_HP_VPD",
        "DER_SUC_VPD",
        "DER_DIS_VPD",
        "GVF",
        "STROKE_SPEED_AVG",
        "FLUID_RATE_VPD",
        # v312
        "DER_DIS_TEMP_VPD",
    )
    BOOLEAN_METRICS = (
        "HYD",
        "HYD_EGAS",
        "WARN1",
        "WARN1_EGAS",
        "WARN2",
        "WARN2_EGAS",
        "MTR",
        "MTR_EGAS",
        "CLR",
        "CLR_EGAS",
        "HTR",
        "HTR_EGAS",
        "AUX_EGAS",
        "PRS",
        "SBF",
    )
    SURFACE_METRICS = (
        "STROKE_LENGTH",
        "PFDW",
        "TYPE_ENUM",
        "TAP_END_DRF",
        "BS_LAG_REAL",
        "StrokeCount",
        "CurrentFillage",
        "SPM_X10",
        "Key",
    )
    COMPRESSION_METRICS = (
        "StrokeCountE",
        "KeyE",
        "STROKE_LENGTH_E",
        "BTM_POS_E",
        "CHECKSUM",
        "CYL_LENGTH",
        "HP_RAISING",
        "HP_LOWERING",
        "DERATE_R",
        "DERATE_L",
        "TYPE_ENUM_E",
    )

    POLL_EVERY_X_SECONDS = 0  # The 'sqs_list_of_dict_msgs' function also waits 10 seconds if the queue is empty
    MAX_NUMBER_OF_MESSAGES = 10
    DELETE_FROM_QUEUE = True
    SQS_QUEUES = (
        "https://sqs.us-west-2.amazonaws.com/960752594355/timescale_all",
        "https://sqs.us-west-2.amazonaws.com/960752594355/timescale_all_rt",
    )
    ROUND_TO_X_MINUTE_INTERVAL = (
        2  # Only store data in X-minute increments to smooth it and reduce the data
    )

    EMAILEES_LIST_DEV = ("smccarthy@myijack.com",)

    def __init__(
        self,
        name=__name__,
        use_file_handler=True,
        log_level_fh=logging.INFO,
        log_level_sh=logging.INFO,
    ):
        """Run when Config() is called"""

        # Initialize the logging
        self.logger = configure_logging(
            name,
            use_file_handler=use_file_handler,
            log_level_fh=log_level_fh,
            log_level_sh=log_level_sh,
        )


def send_to_sqs(QueueUrl, MessageBody):
    # # Create SQS client
    # sqs = boto3.client('sqs', region_name='us-west-2')
    response = sqs.send_message(
        QueueUrl=QueueUrl,
        MessageBody=MessageBody,
        DelaySeconds=0,
    )
    assert response.get("ResponseMetadata", {}).get("HTTPStatusCode", None) == 200
    return response


def round_dt_to_minute_interval(dt, minute_interval=5):
    """Takes a datetime and rounds it to the nearest 'minute_interval'"""

    # Add x minutes, before rounding down
    dt += timedelta(minutes=minute_interval)
    dt = dt - timedelta(
        minutes=dt.minute % minute_interval,
        seconds=dt.second,
        microseconds=dt.microsecond,
    )
    return dt


def get_hourly_datetimes(timestamp_utc_dt):
    """For surface and compression card data, we only need hourly date-times"""

    timestamp_utc_dt_rounded = timestamp_utc_dt.replace(
        minute=0, second=0, microsecond=0
    )
    timestamp_utc_dt_str = datetime.strftime(timestamp_utc_dt_rounded, "%Y-%m-%d %H:%M")

    return timestamp_utc_dt_str


def parse_card_direction_position(metric, surface_or_compression):
    """Given the surface/compression card data "metric name", get the direction and position"""

    # First find whether it's going up or down, from the first letter in the metric (e.g. 'd10' or 'u10')
    up_down = metric[0]
    is_up = True if up_down == "u" else False
    # Next grab the position, after the 'd' or 'u' (i.e. the second character or 2 in 0-based counting)
    position = None
    try:
        if surface_or_compression == "surface":
            position = int(metric[1:])
        else:
            position = int(metric[2:])
    except Exception:
        c.logger.exception(
            f"Unable to get card position from metric: {metric} using 'int(metric[<1 or 2>:])'"
        )
        return None

    return is_up, position


def send_mailgun_email(
    c, text="", html="", emailees_list=[], subject="IJACK Alert", images=None
):
    """Send email using Mailgun"""
    # Initialize the return code
    rc = ""
    if c.TEST_FUNC:
        return rc

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
            "https://api.mailgun.net/v3/mg.ijack.ca/messages",
            auth=("api", os.environ["MAILGUN_API_KEY"]),
            files=images2,
            data={
                "from": "IJACK <smccarthy@ijack.ca>",
                "to": emailees_list,  # never hard-code this
                "subject": subject,
                key: value,
            },
        )
        c.logger.info(
            f"Email sent to emailees_list: '{str(emailees_list)}'. \nSubject: {subject}. \nrc.status_code: {rc.status_code}"
        )

    return rc


def send_email_if_error(c, time_start_last_error_email, extra_msg=None):
    """Send an email if there's an error, but don't send it too frequently"""

    # How long since last error email was sent
    current_ts = datetime.now(timezone.utc).timestamp()
    seconds_since_last_error_email_sent_alerts = (
        current_ts - time_start_last_error_email
    )
    msg = ""  # initialize so we don't get an "UnboundLocalError: local variable 'msg' referenced before assignment"

    if seconds_since_last_error_email_sent_alerts > 60 * 10:
        time_start_last_error_email = current_ts

        subject = "ERROR with Timescale DB inserter.py"

        # Get the error information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_msg_list = traceback.format_exception(exc_type, exc_value, exc_traceback)
        error_msg = ""
        for line in error_msg_list:
            error_msg += line

        msg = f"Here's the error information: \n{error_msg}"
        if extra_msg is not None:
            msg += f"\n\n{extra_msg}"

        send_mailgun_email(
            c, text=msg, html="", emailees_list=c.EMAILEES_LIST_DEV, subject=subject
        )

    return time_start_last_error_email, msg


def execute_sql(
    c, conn, sql, values=None, is_commit=True, is_fetchall=False, is_dataframe=False
):

    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, values)
        except Exception:
            c.logger.exception(f"ERROR running SQL: '{sql}'")
            c.logger.error(f"values we tried to use: '{values}'")
            c.logger.warning("Rolling back transaction now...")
            # Autocommit is off (by default in TimescaleDB, it seems)
            # If we don't rollback() the transaction, no future transactions will be able to run at all. Big problem.
            conn.rollback()
        else:
            c.logger.info(f"SUCCESS: cursor.execute(sql, values=\n{values})")
            if is_commit and not is_dataframe and not is_fetchall:
                # Autocommit is off (by default in TimescaleDB, it seems)
                conn.commit()
                c.logger.debug("SUCCESS: conn.commit() worked")
            if is_fetchall:
                rows = cursor.fetchall()
                if is_dataframe:
                    fields = [str.lower(x[0]) for x in cursor.description]
                    df = pd.DataFrame(rows, columns=fields)
                    return df
                return rows
        finally:
            # I'm not sure if this is necessary since we have the "with" context manager,
            # but better safe than sorry
            cursor.close()

    return 0


def upsert_time_series(
    c, conn_ts, timestamp_utc_inserted, timestamp_utc, gateway, metric, value
):
    """
    https://docs.timescale.com/clustering/using-timescaledb/writing-data#insert
    """
    values = {
        "timestamp_utc_inserted": timestamp_utc_inserted,
        "timestamp_utc": timestamp_utc,
        "gateway": gateway,
        metric: value,
    }
    sql = f"""
        INSERT INTO public.time_series
            (timestamp_utc_inserted, timestamp_utc, gateway, {metric})
            VALUES (%(timestamp_utc_inserted)s, %(timestamp_utc)s, %(gateway)s, %({metric})s)
            ON CONFLICT (timestamp_utc, gateway) DO UPDATE
                SET {metric} = '{value}'
    """
    rc = execute_sql(
        c, conn_ts, sql, values, is_commit=True, is_fetchall=False, is_dataframe=False
    )

    return rc


def insert_surface(
    c, conn, timestamp_utc, timestamp_utc_inserted, gateway, is_up, position, load
):
    """
    https://docs.timescale.com/clustering/using-timescaledb/writing-data#insert
    """
    values = {
        "timestamp_utc": timestamp_utc,
        "gateway": gateway,
        "is_up": is_up,
        "position": position,
        "load": load,
        "timestamp_utc_inserted": timestamp_utc_inserted,
    }
    sql = """
        INSERT INTO public.surface (timestamp_utc, gateway, is_up, position, load, timestamp_utc_inserted)
            VALUES (%(timestamp_utc)s, %(gateway)s, %(is_up)s, %(position)s, %(load)s, %(timestamp_utc_inserted)s)
    """
    rc = execute_sql(
        c, conn, sql, values, is_commit=True, is_fetchall=False, is_dataframe=False
    )

    return rc


def insert_meta(
    c,
    conn_ts,
    table_name,
    timestamp_utc,
    timestamp_utc_inserted,
    gateway,
    metric,
    value,
):
    """
    https://docs.timescale.com/clustering/using-timescaledb/writing-data#insert
    """
    values = {
        "timestamp_utc": timestamp_utc,
        "gateway": gateway,
        "metric": metric,
        "value": value,
        "timestamp_utc_inserted": timestamp_utc_inserted,
    }
    sql = f"""
        INSERT INTO public.{table_name} (timestamp_utc, gateway, metric, value, timestamp_utc_inserted)
            VALUES (%(timestamp_utc)s, %(gateway)s, %(metric)s, %(value)s, %(timestamp_utc_inserted)s)
    """
    rc = execute_sql(
        c, conn_ts, sql, values, is_commit=True, is_fetchall=False, is_dataframe=False
    )

    return rc


def insert_compression(
    c, conn, timestamp_utc, timestamp_utc_inserted, gateway, is_up, position, load
):
    """
    https://docs.timescale.com/clustering/using-timescaledb/writing-data#insert
    """
    values = {
        "timestamp_utc": timestamp_utc,
        "gateway": gateway,
        "is_up": is_up,
        "position": position,
        "load": load,
        "timestamp_utc_inserted": timestamp_utc_inserted,
    }
    sql = """
        INSERT INTO public.compression (timestamp_utc, gateway, is_up, position, load, timestamp_utc_inserted)
            VALUES (%(timestamp_utc)s, %(gateway)s, %(is_up)s, %(position)s, %(load)s, %(timestamp_utc_inserted)s)
    """
    # if gateway == '1000028':
    #     print("found the gateway")
    rc = execute_sql(
        c, conn, sql, values, is_commit=True, is_fetchall=False, is_dataframe=False
    )

    return rc


def insert_alarm_log_rds(
    c, conn_rds, timestamp_local, timestamp_utc_inserted, gateway, abbrev, value
):
    """
    Insert the alarm log entries (with their local time)
    into AWS RDS PostgreSQL database
    """
    values = {
        "timestamp_utc_inserted": timestamp_utc_inserted,
        "timestamp_local": timestamp_local,
        "gateway": gateway,
        "abbrev": abbrev,
        "value": value,
    }
    sql = """
        insert into public.alarm_log 
        (timestamp_utc_inserted, timestamp_local, gateway, abbrev, value) 
        values (%(timestamp_utc_inserted)s, %(timestamp_local)s, %(gateway)s, %(abbrev)s, %(value)s)
    """
    rc = execute_sql(
        c, conn_rds, sql, values, is_commit=True, is_fetchall=False, is_dataframe=False
    )

    return rc


@error_wrapper()
def main(c):
    """"""

    # exit_if_already_running(c, pathlib.Path(__file__).name)

    # # Keep a connection open for efficiency
    # # (these get shut down by my process that closes old connections...)
    # conn_ijack = get_conn(c, "ijack")
    # conn_ts = get_conn(c, "timescale")

    try:
        time_start = time.time()

        # Get gateway records
        sql_gw = """
            select 
                distinct on (gateway)
                t1.gateway,
                t1.power_unit_id,
                t2.power_unit,
                t3.id as structure_id,
                t3.structure,
                t3.unit_type_id,
                t4.unit_type
            from public.gw t1
            left join public.power_units t2 
                on t2.id = t1.power_unit_id
            left join public.structures t3
                on t3.power_unit_id = t2.id
            left join myijack.unit_types t4
                on t4.id = t3.unit_type_id
            where t1.backfilled = false
        """
        _, gw_rows = run_query(c, sql_gw, db="ijack", fetchall=True)

        for i, dict_ in enumerate(gw_rows):
            gateway = dict_["gateway"]
            power_unit = dict_["power_unit"]
            unit_type = dict_["unit_type"]
            unit_info_str = f"{unit_type} {gateway} ({power_unit})"
            if unit_type is None or power_unit is None:
                c.logger.warning(
                    f"{unit_info_str} unit_type or power_unit is None. Skipping this gateway!"
                )
                continue
            unit_type_lower = unit_type.lower()
            c.logger.info(f"Gateway {i} of {len(gw_rows)} gw_rows: {unit_info_str}")

            # # Warning for the user, in case she started this program by accident. This is a chance to cancel.
            # yes_or_skip = input(f"Are you sure you want to backfill all time series data for {unit_info_str}? \n(y)es or (s)kip: ")
            # y_or_s_lower = str(yes_or_skip).lower()[0]
            # if y_or_s_lower == "y":
            #     c.logger.info(f"Continuing with {unit_info_str}...")
            # elif y_or_s_lower == "s":
            #     c.logger.warning(f"Skipping {unit_info_str}!")
            #     continue

            c.logger.info(
                f"Finding oldest timestamp from TimescaleDB public.time_series for {unit_info_str}..."
            )
            sql_non_surface = f"""
                select min(timestamp_utc) as min_ts_utc
                from public.time_series
                where gateway = '{gateway}'
            """
            _, time_series_rows = run_query(
                c, sql_non_surface, db="timescale", fetchall=True
            )
            min_timescaledb_timestamp_utc = time_series_rows[0]["min_ts_utc"]
            c.logger.info(
                f"min_timescaledb_timestamp_utc: {min_timescaledb_timestamp_utc}"
            )
            if min_timescaledb_timestamp_utc is None:
                c.logger.warning(
                    f"min_timescaledb_timestamp_utc '{min_timescaledb_timestamp_utc}' is None. Skipping this gateway!"
                )
                continue

            # Start with the non_surface table for regular non-card time series data #########################
            c.logger.info(f"Querying for non-surface data for {unit_info_str}...")
            sql_non_surface = f"""
                select 
                    gateway, timestamp_utc, abbrev, value
                from public.non_surface
                where gateway = '{gateway}'
                    and timestamp_utc < '{min_timescaledb_timestamp_utc}'
                --limit 1
            """
            start_ts_non_surface = time.time()
            _, non_surface_rows = run_query(
                c, sql_non_surface, db="ijack", fetchall=True
            )
            finish_ts_non_surface = time.time()
            c.logger.info(
                f"Time to query non_surface table: {round((finish_ts_non_surface - start_ts_non_surface)/60)} minutes"
            )
            n_non_surface_rows = len(non_surface_rows)
            c.logger.info(
                f"non_surface rows to upload to SQS queue: {n_non_surface_rows}"
            )

            # Send rows to SQS queue, to be picked up by the TimescaleDB inserter.py Docker container

            # conn_ts = get_conn(c, "timescale")
            # conn_rds = get_conn(c, "ijack")

            def parse_and_send_non_surface(ns_dict):
                """
                Uploads to AWS SQS queue.
                Intented to run in a thread using concurrent.futures.ThreadPoolExecutor
                """
                gateway = ns_dict["gateway"]
                timestamp_utc_dt = ns_dict["timestamp_utc"]
                sent_on = int(timestamp_utc_dt.timestamp() * 1000)
                metric = ns_dict["abbrev"]
                value = ns_dict["value"]

                # # for TimescaleDB direct insertions
                # timestamp_utc_dt_str = str(timestamp_utc_dt)
                # timestamp_utc_inserted_str = timestamp_utc_dt_str

                payload = {
                    "mqtt_unit_id": gateway,
                    "sent_on": sent_on,
                    "metrics": {metric: value},
                }
                payload_json = json.dumps(payload)
                send_to_sqs(QueueUrl=sqs_queue_url, MessageBody=payload_json)

            # for k, ns_dict in enumerate(non_surface_rows):
            #     c.logger.info(f"Parsing {k} of {n_non_surface_rows} non_surface_rows for {unit_info_str}")
            #     gateway = ns_dict["gateway"]
            #     timestamp_utc_dt = ns_dict["timestamp_utc"]
            #     timestamp_utc_dt_str = str(timestamp_utc_dt)
            #     timestamp_utc_inserted_str = timestamp_utc_dt_str
            #     sent_on = int(timestamp_utc_dt.timestamp() * 1000)
            #     metric = ns_dict["abbrev"]
            #     value = ns_dict["value"]

            #     # if metric in c.TIME_SERIES_METRICS:
            #     #     if metric in ("OHE", "OHU"):
            #     #         metric = 'oh'
            #     #     if metric in ("SHE", "SHU"):
            #     #         metric = 'sh'
            #     #     upsert_time_series(
            #     #         c=c,
            #     #         conn_ts=conn_ts,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         timestamp_utc=timestamp_utc_dt_str,
            #     #         gateway=gateway,
            #     #         metric=metric,
            #     #         value=value
            #     #     )

            #     # elif metric in c.BOOLEAN_METRICS:
            #     #     # Change boolean values to True/False to avoid an error on insert to TimescaleDB
            #     #     if value in ('1', 1):
            #     #         value = True
            #     #     elif value in ('0', 0):
            #     #         value = False
            #     #     upsert_time_series(
            #     #         c=c,
            #     #         conn_ts=conn_ts,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         timestamp_utc=timestamp_utc_dt_str,
            #     #         gateway=gateway,
            #     #         metric=metric,
            #     #         value=value
            #     #     )

            #     # elif metric in c.SURFACE_METRICS:
            #     #     timestamp_utc_dt_str = get_hourly_datetimes(timestamp_utc_dt)
            #     #     insert_meta(
            #     #         c=c,
            #     #         conn_ts=conn_ts,
            #     #         table_name="surface_meta",
            #     #         timestamp_utc=timestamp_utc_dt_str,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         gateway=gateway,
            #     #         metric=metric,
            #     #         value=value
            #     #     )

            #     # elif metric in c.COMPRESSION_METRICS:
            #     #     timestamp_utc_dt_str = get_hourly_datetimes(timestamp_utc_dt)
            #     #     insert_meta(
            #     #         c=c,
            #     #         conn_ts=conn_ts,
            #     #         table_name="compression_meta",
            #     #         timestamp_utc=timestamp_utc_dt_str,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         gateway=gateway,
            #     #         metric=metric,
            #     #         value=value
            #     #     )

            #     # # Alarm log values are actually timestamps, not strings,
            #     # # so convert to float to avoid an error with the TimescaleDB inserter.py
            #     # elif re.match("^[A][EU][_][0-9]{1,3}\\b", metric):
            #     #     value_num = float(value)
            #     #     timestamp_local_str = str(datetime.utcfromtimestamp(int(value_num / 1000)))

            #     #     insert_alarm_log_rds(
            #     #         c=c,
            #     #         conn_rds=conn_rds,
            #     #         timestamp_local=timestamp_local_str,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         gateway=gateway,
            #     #         abbrev=metric,
            #     #         value=value
            #     #     )

            #     payload = {
            #         "mqtt_unit_id": gateway,
            #         "sent_on": sent_on,
            #         "metrics": {
            #             metric: value
            #         },
            #     }
            #     payload_json = json.dumps(payload)
            #     send_to_sqs(QueueUrl=sqs_queue_url, MessageBody=payload_json)

            connections = 20

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=connections
            ) as executor:
                c.logger.info(
                    f"Parsing {n_non_surface_rows} non_surface_rows for {unit_info_str}"
                )

                futures = (
                    executor.submit(parse_and_send_non_surface, ns_dict)
                    for ns_dict in non_surface_rows
                )

                time1 = time.time()
                out = []
                for future in concurrent.futures.as_completed(futures):
                    try:
                        data = future.result()
                    except Exception as exc:
                        data = str(type(exc))
                    finally:
                        out.append(data)
                        # print(str(len(out)), end="\r")

                time2 = time.time()

            # print(pd.Series(out).value_counts())
            c.logger.info(f"Number of records processed in non_surface 'out' list: {len(out)}")
            c.logger.info(
                f"Non-surface threaded operations took {(time2-time1)/60:.2f} minutes to post {n_non_surface_rows} rows!"
            )

            # conn_ts.close()
            # conn_rds.close()

            # Now do the surface/compression card data for that gateway ####################################
            if unit_type_lower in ("egas", "xfer"):
                card_table_name = "compression"
            else:
                card_table_name = "surface"
            c.logger.info(
                f"Querying for {unit_type} {card_table_name} card data for {unit_info_str}..."
            )

            sql_card_data = f"""
                select 
                    gateway, timestamp_utc, position, load, up_down
                from public.{card_table_name}
                where gateway = '{gateway}'
                    and timestamp_utc < '{min_timescaledb_timestamp_utc}'
                --limit 1
            """
            start_ts_cards = time.time()
            _, card_data_rows = run_query(c, sql_card_data, db="ijack", fetchall=True)
            finish_ts_cards = time.time()
            c.logger.info(
                f"Time to query cards table: {round((finish_ts_cards - start_ts_cards)/60)} minutes"
            )
            n_card_data_rows = len(card_data_rows)
            c.logger.info(
                f"public.{card_table_name} rows to upload to SQS queue: {n_card_data_rows}"
            )

            # Send rows to SQS queue, to be picked up by the TimescaleDB inserter.py Docker container
            def parse_and_send_card_data_in_thread(card_dict):
                """
                Uploads card data to AWS SQS queue.
                Intented to run in a thread using concurrent.futures.ThreadPoolExecutor
                """
                gateway = card_dict["gateway"]
                dt = card_dict["timestamp_utc"]
                sent_on = int(dt.timestamp() * 1000)
                up_down = card_dict["up_down"]
                position = card_dict["position"]
                load = card_dict["load"]

                if unit_type_lower in ("egas", "xfer"):
                    metric = f"{up_down}e{position}"
                else:
                    metric = f"{up_down}{position}"

                payload = {
                    "mqtt_unit_id": gateway,
                    "sent_on": sent_on,
                    "metrics": {metric: load},
                }
                payload_json = json.dumps(payload)
                send_to_sqs(QueueUrl=sqs_queue_url, MessageBody=payload_json)

            # for j, card_dict in enumerate(card_data_rows):
            #     c.logger.info(f"Parsing {j} of {n_card_data_rows} card_data_rows for {unit_info_str}")
            #     gateway = card_dict["gateway"]
            #     dt = card_dict["timestamp_utc"]
            #     # sent_on = int(dt.timestamp() * 1000)
            #     up_down = card_dict["up_down"]
            #     position = card_dict["position"]
            #     load = card_dict["load"]

            #     if unit_type_lower in ("egas", "xfer"):
            #         metric = f"{up_down}e{position}"
            #         surface_or_compression = "compression"
            #     else:
            #         metric = f"{up_down}{position}"
            #         surface_or_compression = "surface"

            #     is_up, position = parse_card_direction_position(metric, surface_or_compression)

            #     timestamp_utc_dt_str = get_hourly_datetimes(dt)
            #     timestamp_utc_inserted_str = str(dt)

            #     # conn_ts = get_conn(c, "timescale")
            #     # if unit_type_lower in ("egas", "xfer"):
            #     #     insert_compression(
            #     #         c=c,
            #     #         conn=conn_ts,
            #     #         timestamp_utc=timestamp_utc_dt_str,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         gateway=gateway,
            #     #         is_up=is_up,
            #     #         position=position,
            #     #         load=load,
            #     #     )
            #     # else:
            #     #     insert_surface(
            #     #         c=c,
            #     #         conn=conn_ts,
            #     #         timestamp_utc=timestamp_utc_dt_str,
            #     #         timestamp_utc_inserted=timestamp_utc_inserted_str,
            #     #         gateway=gateway,
            #     #         is_up=is_up,
            #     #         position=position,
            #     #         load=load,
            #     #     )
            #     # conn_ts.close()

            #     payload = {
            #         "mqtt_unit_id": gateway,
            #         "sent_on": sent_on,
            #         "metrics": {
            #             metric: load
            #         },
            #     }
            #     payload_json = json.dumps(payload)
            #     send_to_sqs(QueueUrl=sqs_queue_url, MessageBody=payload_json)

            connections = 20

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=connections
            ) as executor:
                c.logger.info(
                    f"Parsing {n_card_data_rows} card_data_rows for {unit_info_str}"
                )

                futures = (
                    executor.submit(parse_and_send_card_data_in_thread, card_dict)
                    for card_dict in card_data_rows
                )

                time1 = time.time()
                out2 = []
                for future in concurrent.futures.as_completed(futures):
                    try:
                        data = future.result()
                    except Exception as exc:
                        data = str(type(exc))
                    finally:
                        out2.append(data)
                        # print(str(len(out2)), end="\r")

                time2 = time.time()
                
            # c.logger.info(pd.Series(out2).value_counts())
            c.logger.info(f"Number of records processed in card data 'out2' list: {len(out2)}")
            c.logger.info(f"Card data threaded operations took {(time2-time1)/60:.2f} minutes to post {n_card_data_rows} rows!")

            # Update the public.gw table once it's complete, so we don't do it again ########################
            sql_gw = f"""
                update public.gw
                set backfilled = true
                where gateway = '{gateway}'
            """
            c.logger.info(
                f"Updating public.gw table 'backfilled' column to 'true' for {unit_info_str}"
            )
            run_query(c, sql_gw, db="ijack", commit=True, fetchall=False)

    except Exception:
        c.logger.exception("Error backfilling time series and card data!")
    finally:
        # c.logger.info("Closing DB connections...")
        # conn_ijack.close()
        # conn_ts.close()
        # del conn_ijack
        # del conn_ts
        # c.logger.info("DB connections closed")
        pass

    time_finish = time.time()
    c.logger.info(
        f"Time to backfill all time series and card data: {round((time_finish - time_start)/60)} minutes"
    )

    return None


if __name__ == "__main__":
    LOG_LEVEL = logging.INFO
    LOGFILE_NAME = "backfill_old_time_series_and_card_data"
    c = Config("inserter", use_file_handler=False)
    main(c)


# import pandas as pd
# from io import StringIO
# from psycopg2.extras import RealDictCursor

# power_unit = "200364.0"
# card_table_name = "surface"
# # df_cards = pd.DataFrame(card_data_rows)
# # assert len(df_cards) == len(card_data_rows)
# # df_cards.to_csv(f"{power_unit}_cards.csv")
# df_cards = pd.read_csv(f"{power_unit}_cards.csv")
# df_cards["timestamp_utc_inserted"] = df_cards["timestamp_utc"]
# # df_cards["timestamp_utc2"] = df_cards["timestamp_utc"].transform(get_hourly_datetimes)
# # timestamp_utc_dt_str = get_hourly_datetimes(timestamp_utc_dt)


# def append_to_aws(df, aws_table, db):
#     conn = get_conn(c, db)
#     try:
#         with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#             # Initialize a string buffer
#             sio = StringIO()
#             # Write the Pandas DataFrame as a csv to the buffer
#             sio.write(df.to_csv(index=None, header=None))
#             # Be sure to reset the position to the start of the stream
#             sio.seek(0)
#             cursor.copy_from(
#                 file=sio, table=aws_table, sep=",", null="", size=8192, columns=df.columns
#             )
#     except Exception:
#         c.logger.exception("Trouble copying data!")
#     else:
#         conn.commit()
#     finally:
#         conn.close()

# append_to_aws(df=df_cards, aws_table=f"public.{card_table_name}", db="timescale")
