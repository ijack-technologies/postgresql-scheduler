import boto3
import logging
import os
# import pandas as pd
import json
import time
import pathlib

# local imports
from utils import (
    Config,
    configure_logging,
    run_query,
    error_wrapper,
    send_mailgun_email,
    send_twilio_phone,
    send_twilio_sms,
    get_client_iot,
    error_wrapper,
    exit_if_already_running,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "synch_aws_iot_shadow_with_aws_rds_postgres_config"
c = Config()
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)

@error_wrapper()
def main(c):
    """
    Query unit data from AWS RDS "IJACK" PostgreSQL database, 
    and update it in the AWS IoT "thing shadow" from which the gateways
    will update their local config data. This will be more robust than trying
    to connect to a PostgreSQL database over the internet (too many connections cause errors).
    The AWS IoT thing shadow is more robust, and AWS IoT can accept almost infinite connections at once. 
    """
    
    exit_if_already_running(c, pathlib.Path(__file__).name)

    # These are all the metrics that will be put in the AWS IoT device shadow as "C__{METRIC}"
    SQL = """
        select aws_thing, gateway, customer, mqtt_topic, cust_sub_group_abbrev,
            unit_type, apn, 
            location, power_unit, model, 
            wait_time_mins_spm, 
            time_zone,
            heartbeat_enabled, online_hb_enabled, spm, suction, discharge, hyd_temp, 
            wait_time_mins, wait_time_mins_ol, wait_time_mins_spm, wait_time_mins_suction, wait_time_mins_discharge, wait_time_mins_hyd_temp
        from public.gateways
        where aws_thing <> 'test'
            and aws_thing is not null
            and customer_id != 21 -- demo customer
    """
    _, rows = run_query(c, SQL, db="ijack", fetchall=True)
    # df = pd.DataFrame(rows, columns=columns)

    # Get the Boto3 AWS IoT client for updating the "thing shadow"
    client_iot = get_client_iot()

    n_rows = len(rows)
    time_start = time.time()
    for i, dict_ in enumerate(rows):
        # Initialize a new thing shadow for the data we're going to update in AWS IoT
        d = {'state': {'reported': {}}}

        for key, value in dict_.items():
            if key in ('gateway', 'unit_type', 'aws_thing'):
                d['state']['reported'][f"C__{key.upper()}"] = value.upper()
            else:
                d['state']['reported'][f"C__{key.upper()}"] = value

        # Logger info
        customer = None
        try:
            # Get a slightly shorter customer name, if available
            customer = dict_['mqtt_topic'].title()
        except Exception:
            c.logger.exception("Trouble finding the MQTT topic. Is this the SHOP gateway? Continuing with the customer name instead...")
            customer = dict_['customer']

        aws_thing = dict_['aws_thing'].upper()
        c.logger.info(f"{i+1} of {n_rows}: Updating {customer} AWS_THING: {aws_thing}")

        # Update the thing shadow for this gateway/AWS_THING
        client_iot.update_thing_shadow(thingName=dict_['aws_thing'], payload=json.dumps(d))

    time_finish = time.time()
    c.logger.info(f"Time to update all AWS IoT thing shadows: {round(time_finish - time_start)} seconds")

    return None


if __name__ == "__main__":
    main(c)
