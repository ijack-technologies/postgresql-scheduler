#######################################################################################################
# This module is meant to be run from development only, not from the production server/Docker container
# It updates the gps_lat and gps_lon fields in the public.structures table, using the API from legallandconverter.com
# API instructions here: https://legallandconverter.com/p51.html#OVERVIEW
# It costs USD $0.10 per lookup, so don't be wasteful since there are 500+ lookups ($50)
#######################################################################################################

import sys
import logging
import os
import pathlib
import re
import time
import json
from datetime import datetime
import re

import boto3
import requests
from dotenv import load_dotenv

# local imports
from utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    get_conn,
    run_query,
)

load_dotenv()
LOG_LEVEL = logging.INFO
LOGFILE_NAME = "backfill_old_time_series_and_card_data"
c = Config()
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)

# Warning for the user, in case she started this program by accident. This is a chance to cancel.
yes_or_no = input("Are you sure you want to backfill all time series data? \n(y)es or (n)o: ")
y_or_n_lower = str(yes_or_no).lower()[0]
if y_or_n_lower == "y":
    c.logger.info("Continuing...")
elif y_or_n_lower == "n":
    c.logger.warning("Exiting now!")
    sys.exit()


# Create SQS client
sqs = boto3.client('sqs', region_name='us-west-2')

# sqs_queue_url = 'https://sqs.us-west-2.amazonaws.com/960752594355/test_sqs'
sqs_queue_url = "https://sqs.us-west-2.amazonaws.com/960752594355/timescale_all"

def send_to_sqs(QueueUrl, MessageBody):
    response = sqs.send_message(
        QueueUrl=QueueUrl,
        MessageBody=MessageBody,
        DelaySeconds=0,
    )
    assert response.get("ResponseMetadata", {}).get("HTTPStatusCode", None) == 200
    return response


@error_wrapper()
def main(c):
    """"""

    # exit_if_already_running(c, pathlib.Path(__file__).name)

    # Keep a connection open for efficiency
    conn_ijack = get_conn(c, "ijack")
    conn_ts = get_conn(c, "timescale")

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
        _, gw_rows = run_query(c, sql_gw, db="ijack", fetchall=True, conn=conn_ijack)

        for i, dict_ in enumerate(gw_rows):
            gateway = dict_["gateway"]
            power_unit = dict_["power_unit"]
            unit_type = dict_["unit_type"]
            unit_type_lower = unit_type.lower()

            unit_info_str = f"{gateway} ({power_unit})"
            
            # # Warning for the user, in case she started this program by accident. This is a chance to cancel.
            # yes_or_skip = input(f"Are you sure you want to backfill all time series data for {unit_info_str}? \n(y)es or (s)kip: ")
            # y_or_s_lower = str(yes_or_skip).lower()[0]
            # if y_or_s_lower == "y":
            #     c.logger.info(f"Continuing with {unit_info_str}...")
            # elif y_or_s_lower == "s":
            #     c.logger.warning(f"Skipping {unit_info_str}!")
            #     continue

            c.logger.info(f"Finding oldest timestamp from TimescaleDB public.time_series for {unit_info_str}...")
            sql_non_surface = f"""
                select min(timestamp_utc) as min_ts_utc
                from public.time_series
                where gateway = '{gateway}'
            """
            _, time_series_rows = run_query(c, sql_non_surface, db="timescale", fetchall=True, conn=conn_ts)
            min_timescaledb_timestamp_utc = time_series_rows[0]["min_ts_utc"]
            c.logger.info(f"min_timescaledb_timestamp_utc: {min_timescaledb_timestamp_utc}")


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
            _, non_surface_rows = run_query(c, sql_non_surface, db="ijack", fetchall=True, conn=conn_ijack)
            finish_ts_non_surface = time.time()
            c.logger.info(
                f"Time to query non_surface table: {round((finish_ts_non_surface - start_ts_non_surface)/60)} minutes"
            )
            c.logger.info(f"non_surface rows to upload to SQS queue: {len(non_surface_rows)}")

            # Send rows to SQS queue, to be picked up by the TimescaleDB inserter.py Docker container
            for k, ns_dict in enumerate(non_surface_rows):
                gateway = ns_dict["gateway"]
                dt = ns_dict["timestamp_utc"]
                sent_on = int(dt.timestamp() * 1000)
                metric = ns_dict["abbrev"]
                value = ns_dict["value"]

                # Alarm log values are actually timestamps, not strings,
                # so convert to float to avoid an error with the TimescaleDB inserter.py
                if re.match("^[A][EU][_][0-9]{1,3}\\b", metric):
                    value = float(value)

                payload = {
                    "mqtt_unit_id": gateway,
                    "sent_on": sent_on,
                    "metrics": {
                        metric: value
                    },
                }
                payload_json = json.dumps(payload)
                send_to_sqs(QueueUrl=sqs_queue_url, MessageBody=payload_json)
                
            
            # Now do the surface/compression card data for that gateway ####################################
            if unit_type_lower in ("egas", "xfer"):
                card_table_name = "compression"
            else:
                card_table_name = "surface"
            c.logger.info(f"Querying for {unit_type} {card_table_name} card data for {unit_info_str}...")

            sql_card_data = f"""
                select 
                    gateway, timestamp_utc, position, load, up_down
                from public.{card_table_name}
                where gateway = '{gateway}'
                    and timestamp_utc < '{min_timescaledb_timestamp_utc}'
                --limit 1
            """
            start_ts_cards = time.time()
            _, card_data_rows = run_query(c, sql_card_data, db="ijack", fetchall=True, conn=conn_ijack)
            finish_ts_cards = time.time()
            c.logger.info(
                f"Time to query cards table: {round((finish_ts_cards - start_ts_cards)/60)} minutes"
            )
            c.logger.info(f"public.{card_table_name} rows to upload to SQS queue: {len(card_data_rows)}")

            # Send rows to SQS queue, to be picked up by the TimescaleDB inserter.py Docker container
            for j, card_dict in enumerate(card_data_rows):
                gateway = card_dict["gateway"]
                dt = card_dict["timestamp_utc"]
                sent_on = int(dt.timestamp() * 1000)
                up_down = card_dict["up_down"]
                position = card_dict["position"]
                value = card_dict["load"]

                if unit_type_lower in ("egas", "xfer"):
                    metric = f"{up_down}e{position}"
                else:
                    metric = f"{up_down}{position}"

                payload = {
                    "mqtt_unit_id": gateway,
                    "sent_on": sent_on,
                    "metrics": {
                        metric: value
                    },
                }
                payload_json = json.dumps(payload)
                send_to_sqs(QueueUrl=sqs_queue_url, MessageBody=payload_json)


            # Update the public.gw table once it's complete, so we don't do it again ########################
            sql_gw = f"""
                update public.gw
                set backfilled = true
                where gateway = '{gateway}'
            """
            c.logger.info(f"Updating public.gw table 'backfilled' column to 'true' for {unit_info_str}")
            _, gw_rows = run_query(c, sql_gw, db="ijack", commit=True, fetchall=False, conn=conn_ijack)

    except Exception:
        c.logger.exception("Error backfilling time series and card data!")
    finally:
        c.logger.info("Closing DB connections...")
        conn_ijack.close()
        conn_ts.close()
        del conn_ijack
        del conn_ts
        c.logger.info("DB connections closed")

    time_finish = time.time()
    c.logger.info(
        f"Time to backfill all time series and card data: {round((time_finish - time_start)/60)} minutes"
    )

    return None


if __name__ == "__main__":
    main(c)
