import boto3
import logging
import os
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
    get_iot_device_shadow,
    exit_if_already_running,
    error_wrapper,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "update_gw_power_unit_id_from_shadow"


def convert_to_float(c, string):
    try:
        return float(string)
    except Exception:
        c.logger.info(f"Cannot convert '{string}' to float...")
        return 0


def update_structures_table(c, power_unit_id, column, new_value, structure, db_value):
    sql_update = f"""
        update public.structures
        set {column} = {new_value}
        where power_unit_id = {power_unit_id}
            -- structure = {structure}
            -- previous value = {db_value}
    """
    run_query(c, sql_update, db="ijack", fetchall=False, commit=True)
    subject = "Changing GPS in public.structures table!"
    send_mailgun_email(c, text=sql_update, html='', emailees_list=c.EMAIL_LIST_DEV, subject=subject)


def compare_shadow_and_db(c, shadow_value, db_value, db_column, power_unit_id, structure):
    if shadow_value is None:
        return None
    shadow_value2 = round(convert_to_float(c, shadow_value), 2)
    db_value2 = round(convert_to_float(c, db_value), 2)
    if shadow_value2 != 0 and shadow_value2 != db_value2:
        update_structures_table(c, power_unit_id, db_column, shadow_value, structure, db_value)


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

    # Get gateway records
    sql_gw = """
        select 
            distinct on (aws_thing)
            t1.aws_thing, 
            t1.power_unit_id, 
            t2.power_unit
        from public.gw t1
        left join public.power_units t2 
            on t2.id = t1.power_unit_id
        --where aws_thing <> 'test'
        --    and aws_thing is not null
        --    and power_unit_id is not null
    """
    _, gw_rows = run_query(c, sql_gw, db="ijack", fetchall=True)

    # Get power_unit records
    sql_pu = """
        select 
            id as power_unit_id, 
            power_unit
        from public.power_units
    """
    _, pu_rows = run_query(c, sql_pu, db="ijack", fetchall=True)
    pu_dict = {row['power_unit']: row['power_unit_id'] for row in pu_rows}

    # Get structure records for comparing GPS lat/lon by power unit
    sql_structures = """
        select 
            structure,
            power_unit_id,
            gps_lat,
            gps_lon
        from public.structures
        where power_unit_id is not null
    """
    _, structure_rows = run_query(c, sql_structures, db="ijack", fetchall=True)

    # Get the Boto3 AWS IoT client for updating the "thing shadow"
    client_iot = get_client_iot()

    n_gw_rows = len(gw_rows)
    time_start = time.time()
    for i, dict_ in enumerate(gw_rows):
        aws_thing = dict_.get('aws_thing', None)

        # This "if aws_thing is None" is unnecessary since the nulls are filtered out in the query, 
        # and simply not allowed in the table, but it doesn't hurt
        if aws_thing is None:
            c.logger.warning('"AWS thing" is None. Continuing with next aws_thing in public.gw table...')
            continue

        shadow = get_iot_device_shadow(c, client_iot, aws_thing)
        if shadow == {}:
            c.logger.warning(f'No shadow exists for aws_thing "{aws_thing}". Continuing with next AWS_THING in public.gw table...')
            continue

        reported = shadow.get('state', {}).get('reported', {})
        power_unit_shadow = reported.get('SERIAL_NUMBER', None)
        latitude_shadow = reported.get('LATITUDE', None)
        longitude_shadow = reported.get('LONGITUDE', None)

        if power_unit_shadow is None:
            c.logger.warning(f'Power unit "SERIAL_NUMBER" not in shadow for aws_thing "{aws_thing}". Continuing with next AWS_THING in public.gw table...')
            continue

        try:
            power_unit_shadow = int(power_unit_shadow)
        except Exception:
            c.logger.exception(f"Can't convert the '{aws_thing}' device shadow's power_unit of '{power_unit_shadow}' to an integer. \
Continuing with next AWS_THING in public.gw table...")
            continue

        power_unit_id_shadow = pu_dict.get(power_unit_shadow, None)
        if power_unit_id_shadow is None:
            c.logger.warning(f"Can't find the power unit ID for the shadow's reported power unit of '{power_unit_shadow}'. \
Continuing with next AWS_THING in public.gw table...")
            continue

        # Get the power_unit_id already in the public.gw table
        power_unit_id_gw = dict_.get('power_unit_id', None)

        # Compare the GPS first
        structure_rows_relevant = [row for row in structure_rows if row['power_unit_id'] == power_unit_id_gw]
        for row in structure_rows_relevant:
            structure = row['structure']
            compare_shadow_and_db(c, latitude_shadow, row['gps_lat'], 'gps_lat', power_unit_id_gw, structure)
            compare_shadow_and_db(c, longitude_shadow, row['gps_lon'], 'gps_lon', power_unit_id_gw, structure)

        if power_unit_id_shadow == power_unit_id_gw:
            c.logger.info(f"Power unit '{power_unit_shadow}' in the public.gw table matches the one reported in the device shadow. Continuing with next...")
            continue

        power_unit_gw = dict_.get('power_unit', None)
        
        msg = f"{i+1} of {n_gw_rows}: Updating public.gw AWS_THING: {aws_thing} to \
power unit: {power_unit_shadow} ({power_unit_id_shadow}) instead of: {power_unit_gw}"
        c.logger.info(msg)

        subject = "Changing power unit in public.gw table!"
        send_mailgun_email(c, text=msg, html='', emailees_list=c.EMAIL_LIST_DEV, subject=subject)

    time_finish = time.time()
    c.logger.info(f"Time to update all gateways to their reported power units: {round(time_finish - time_start)} seconds")

    return None


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
