import logging
import pathlib
import time
import sys

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = str(pathlib.Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

# local imports
from cron_d.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    get_client_iot,
    get_conn,
    get_iot_device_shadow,
    run_query,
    send_mailgun_email,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "update_gw_power_unit_id_from_shadow"


def convert_to_float(c, string):
    try:
        return float(string)
    except Exception:
        c.logger.info(f"Cannot convert '{string}' to float...")
        return 0


def sql_get_info(power_unit_id, power_unit_shadow, structure, aws_thing):
    return f"""
        SELECT id, structure, structure_slave_id, structure_slave, 
            downhole, surface, location, gps_lat, gps_lon, 
            power_unit_id, power_unit, power_unit_str, 
            gateway_id, gateway, aws_thing, qb_sale, unit_type_id, unit_type, 
            model_type_id, model, model_unit_type_id, model_unit_type, 
            model_type_id_slave, model_slave, model_unit_type_id_slave, model_unit_type_slave, 
            customer_id, customer, cust_sub_group_id, cust_sub_group, 
            run_mfg_date, structure_install_date, slave_install_date, 
            notes_1, well_license, time_zone_id, time_zone, apn
	    FROM public.vw_structures_joined
        where power_unit_id = {power_unit_id}
            and customer_id != 21 --demo customer
            -- power unit reported in the AWS IoT device shadow:
            -- power_unit = {power_unit_shadow}
            -- structure from public.structures table,
            -- based on power_unit_id associated with aws_thing in public.gw table
            -- structure = {structure}
            -- aws_thing = {aws_thing}
        limit 1
    """


def get_sql_update(
    column,
    new_value,
    db_value,
    power_unit_id,
    dict_,
    power_unit_shadow,
    structure,
    aws_thing,
):
    return f"""
        update public.structures
        set {column} = {new_value}
        -- previous value = {db_value}
        where power_unit_id = {power_unit_id}
            -- customer = {dict_["customer"]}
            -- model = {dict_["model"]}
            -- surface = {dict_["surface"]}
            -- cust_sub_group = {dict_["cust_sub_group"]}
            -- power unit reported in the AWS IoT device shadow:
            -- power_unit = {power_unit_shadow}
            -- structure from public.structures table,
            -- based on power_unit_id associated with aws_thing in public.gw table
            -- structure = {structure}
            -- aws_thing = {aws_thing}
            -- notes_1 = {dict_["notes_1"]}
    """


def get_html(power_unit_shadow, sql_update, dict_):
    return f"""
        <html>
        <body>

        <h1>Running the following SQL to update the public.structures table!</h1>
        <br>
        <a href="https://myijack.com/rcom?unit={power_unit_shadow}">https://myijack.com/rcom?unit={power_unit_shadow}</a>
        <br>
        <p>{sql_update}</p>
        <p>{dict_}</p>

        </body>
        </html>
    """


def update_structures_table(
    c,
    power_unit_id: int,
    power_unit_shadow: int,
    column: str,
    new_value,
    structure: int,
    aws_thing: str,
    db_value,
    # for testing
    execute: bool = True,
) -> None:
    """Actually update the public.structures table, and send an email alert about it"""

    # Gather a bit more info for the email alert
    sql_get_info_str = sql_get_info(
        power_unit_id, power_unit_shadow, structure, aws_thing
    )
    _, rows = run_query(
        c, sql_get_info_str, db="ijack", execute=True, fetchall=True, commit=False
    )
    dict_ = rows[0]

    sql_update = get_sql_update(
        column,
        new_value,
        db_value,
        power_unit_id,
        dict_,
        power_unit_shadow,
        structure,
        aws_thing,
    )

    html = get_html(power_unit_shadow, sql_update, dict_)

    # Just send a warning instead of auto-updating?
    subject = "Updating GPS in public.structures table!"
    send_mailgun_email(
        c, text="", html=html, emailees_list=c.EMAIL_LIST_DEV, subject=subject
    )
    # Don't run this automatically since it undoes my manual updates with the
    # test_update_gps_lat_lon_from_land_locations.py program which cost $0.10/per lookup
    run_query(c, sql_update, db="ijack", fetchall=False, execute=execute)

    return None


def compare_shadow_and_db(
    c,
    shadow_value,
    db_value,
    db_column,
    power_unit_id,
    power_unit_shadow,
    structure,
    aws_thing,
):
    if shadow_value is None:
        return None
    shadow_value2 = round(convert_to_float(c, shadow_value), 2)
    db_value2 = round(convert_to_float(c, db_value), 2)
    if shadow_value2 != 0 and shadow_value2 != db_value2:
        # The following program could potentially change the database,
        # but it just emails me a warning instead
        update_structures_table(
            c,
            power_unit_id,
            power_unit_shadow,
            db_column,
            shadow_value,
            structure,
            aws_thing,
            db_value,
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

    # Get DB connection since we're running several queries (might as well have just one connection)
    conn = get_conn(c, db="ijack")

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
    _, gw_rows = run_query(c, sql_gw, db="ijack", fetchall=True, conn=conn)

    # Get power_unit records
    sql_pu = """
        select 
            id as power_unit_id, 
            power_unit
        from public.power_units
    """
    _, pu_rows = run_query(c, sql_pu, db="ijack", fetchall=True, conn=conn)
    pu_dict = {row["power_unit"]: row["power_unit_id"] for row in pu_rows}

    # Get structure records for comparing GPS lat/lon by power unit
    sql_structures = """
        select 
            structure,
            power_unit_id,
            gps_lat,
            gps_lon,
            t3.customer
        from public.structures t1
        left join myijack.structure_customer_rel t2
            on t2.structure_id = t1.id
        left join myijack.customers t3
            on t3.id = t2.customer_id
        where power_unit_id is not null
    """
    _, structure_rows = run_query(
        c, sql_structures, db="ijack", fetchall=True, conn=conn
    )

    # Close the DB connection now
    conn.close()
    del conn

    # Get the Boto3 AWS IoT client for updating the "thing shadow"
    client_iot = get_client_iot()

    n_gw_rows = len(gw_rows)
    time_start = time.time()
    for i, dict_ in enumerate(gw_rows):
        aws_thing = dict_.get("aws_thing", None)

        # This "if aws_thing is None" is unnecessary since the nulls are filtered out in the query,
        # and simply not allowed in the table, but it doesn't hurt
        if aws_thing is None:
            c.logger.warning(
                '"AWS thing" is None. Continuing with next aws_thing in public.gw table...'
            )
            continue

        shadow = get_iot_device_shadow(c, client_iot, aws_thing)
        if shadow == {}:
            c.logger.warning(
                f'No shadow exists for aws_thing "{aws_thing}". Continuing with next AWS_THING in public.gw table...'
            )
            continue

        reported = shadow.get("state", {}).get("reported", {})
        power_unit_shadow = reported.get("SERIAL_NUMBER", None)
        latitude_shadow = reported.get("LATITUDE", None)
        longitude_shadow = reported.get("LONGITUDE", None)

        if power_unit_shadow is None:
            c.logger.warning(
                f'Power unit "SERIAL_NUMBER" not in shadow for aws_thing "{aws_thing}". Continuing with next AWS_THING in public.gw table...'
            )
            continue

        try:
            power_unit_shadow = int(power_unit_shadow)
        except Exception:
            c.logger.exception(
                f"Can't convert the '{aws_thing}' device shadow's power_unit of '{power_unit_shadow}' to an integer. \
Continuing with next AWS_THING in public.gw table..."
            )
            continue

        power_unit_id_shadow = pu_dict.get(power_unit_shadow, None)
        if power_unit_id_shadow is None:
            c.logger.warning(
                f"Can't find the power unit ID for the shadow's reported power unit of '{power_unit_shadow}'. \
Continuing with next AWS_THING in public.gw table..."
            )
            continue

        # Get the power_unit_id already in the public.gw table
        power_unit_id_gw = dict_.get("power_unit_id", None)

        # Compare the GPS first
        structure = None
        customer = None
        structure_rows_relevant = [
            row for row in structure_rows if row["power_unit_id"] == power_unit_id_gw
        ]
        for row in structure_rows_relevant:
            structure = row["structure"]
            customer = row["customer"]
            compare_shadow_and_db(
                c,
                latitude_shadow,
                row["gps_lat"],
                "gps_lat",
                power_unit_id_gw,
                power_unit_shadow,
                structure,
                aws_thing,
            )
            compare_shadow_and_db(
                c,
                longitude_shadow,
                row["gps_lon"],
                "gps_lon",
                power_unit_id_gw,
                power_unit_shadow,
                structure,
                aws_thing,
            )

        if power_unit_id_shadow == power_unit_id_gw:
            c.logger.info(
                f"Power unit '{power_unit_shadow}' in the public.gw table matches the one reported in the device shadow. Continuing with next..."
            )
            continue

        power_unit_gw = dict_.get("power_unit", None)

        msg = (
            f"{i+1} of {n_gw_rows}: Please update public.gw AWS_THING: {aws_thing} record to "
            + f"the power unit reported in the shadow: '{power_unit_shadow}' ({power_unit_id_shadow}) "
            + f"instead of '{power_unit_gw}' ({power_unit_id_gw}) in the public.gw table. "
            + f"The structure for the current power unit ID of '{power_unit_id_gw}' is '{structure}'. "
            + f"The customer for structure '{structure}' is '{customer}'. "
        )
        c.logger.info(msg)

        subject = "Need new/different power unit in public.gw table!"
        send_mailgun_email(
            c, text=msg, html="", emailees_list=c.EMAIL_LIST_DEV, subject=subject
        )

    time_finish = time.time()
    c.logger.info(
        f"Time to update all gateways to their reported power units: {round(time_finish - time_start)} seconds"
    )

    del client_iot

    return None


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
