from datetime import date, datetime, timedelta
import logging
import pathlib
import time
import sys
import pprint
import pickle
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from psycopg2 import connect as psycopg2_connect
from numbers import Number

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
    seconds_since_last_any_msg,
    send_mailgun_email,
    utc_timestamp_to_datetime_string,
)
# from test.fixtures.fixture_utils import save_fixture

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "update_gw_power_unit_id_from_shadow"


def convert_to_float(c, string):
    try:
        return float(string)
    except Exception:
        c.logger.info("Cannot convert '%s' to float...", string)
        return 0


def sql_get_info(c, power_unit_id, power_unit_shadow, structure, aws_thing):
    """Gather a bit more info for the email alert"""

    select_sql = f"""
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
            and customer_id is distinct from 21 --demo customer
        limit 1;
    """

    # Just for logging
    log_msg = f"""
power unit reported in the AWS IoT device shadow:
power_unit = {power_unit_shadow}
structure from public.structures table,
based on power_unit_id associated with aws_thing in public.gw table
structure = {structure}
aws_thing = {aws_thing}
    """
    c.logger.info(log_msg)
    c.logger.info(f"Select SQL: {select_sql}")

    return select_sql


def get_sql_update(
    c,
    column,
    new_value,
    db_value,
    power_unit_id,
    dict_,
    power_unit_shadow,
    structure,
    aws_thing,
):
    """Get the SQL for updating the structures table"""

    update_sql = f"""
        update public.structures
        set {column} = {new_value}
        -- previous value = {db_value}
        where power_unit_id = {power_unit_id};"""

    # Just for logging
    customer = str(dict_["customer"]).strip().replace("\n", ". ")
    cust_sub_group = str(dict_["cust_sub_group"]).strip().replace("\n", ". ")
    surface = str(dict_["surface"]).strip().replace("\n", ". ")
    model = str(dict_["model"]).strip().replace("\n", ". ")

    log_msg = f"""
customer = {customer}
surface = {surface}
power_unit = {power_unit_shadow} from AWS IoT
model = {model}
cust_sub_group = {cust_sub_group}
structure from public.structures table,
based on power_unit_id associated with aws_thing in public.gw table:
structure = {structure}
aws_thing = {aws_thing}
    """
    c.logger.info(log_msg)
    c.logger.info(f"Update SQL: {update_sql}")

    return update_sql


def get_html(power_unit_shadow, sql_update, dict_):
    """Get HTML for the email"""

    unit_str = f'{dict_["customer"]} {dict_["surface"]} {power_unit_shadow}'
    html = f"""
        <html>
        <body>

        <h2>{unit_str}</h2>
        <a href="https://myijack.com/rcom?unit={power_unit_shadow}">https://myijack.com/rcom?unit={power_unit_shadow}</a>
        <h2>Running the following SQL to update the public.structures table!</h2>
        <br>
        <br>
        <h3>SQL Update Text</h3>
        <p>{str(sql_update).replace(chr(10), "<br>")}</p>
        <br>
        <br>
        <h3>Dictionary Contents</h3>
        <p>{str(pprint.pformat(dict_)).replace(chr(10), "<br>")}</p>

        </body>
        </html>
    """
    return html


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
    commit: bool = False,
) -> None:
    """Actually update the public.structures table, and send an email alert about it"""

    # Gather a bit more info for the email alert
    sql_get_info_str = sql_get_info(
        c, power_unit_id, power_unit_shadow, structure, aws_thing
    )
    _, rows = run_query(
        c, sql_get_info_str, db="ijack", execute=True, fetchall=True, commit=False
    )
    dict_ = rows[0]

    sql_update = get_sql_update(
        c,
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
    if commit:
        subject = "Updating GPS in structures table!"
    else:
        subject = "NOT updating GPS in structures table - just testing!"
    send_mailgun_email(
        c, text="", html=html, emailees_list=c.EMAIL_LIST_DEV, subject=subject
    )
    # Don't run this automatically since it undoes my manual updates with the
    # test_update_gps_lat_lon_from_land_locations.py program which cost $0.10/per lookup
    run_query(c, sql_update, db="ijack", fetchall=False, execute=execute, commit=commit)

    return None


def compare_shadow_and_db(
    c,
    shadow_value: float,
    db_value: float,
    db_column: str,
    power_unit_id: int,
    power_unit_shadow: int,
    structure: int,
    aws_thing: str,
    allow_zero: bool = False,
    commit: bool = False,
):
    """
    Compare the shadow and database values,
    and if they're significantly different, update the database
    """
    if not shadow_value:
        return None

    if not allow_zero and shadow_value == 0:
        return None

    db_value = 0 if db_value is None else db_value
    # Convert to floats so we can compare them mathematically
    try:
        shadow_value = float(shadow_value)
        db_value = db_value if isinstance(db_value, Number) else float(db_value)
    except Exception:
        c.logger.error(
            f"Error converting either shadow_value '{shadow_value}' or db_value '{db_value}' to float"
        )
        return None

    if abs(shadow_value - db_value) > 0.01:
        update_structures_table(
            c,
            power_unit_id,
            power_unit_shadow,
            db_column,
            shadow_value,
            structure,
            aws_thing,
            db_value,
            commit=commit,
        )


def is_power_unit_already_in_use(c, power_unit_id: int) -> Tuple[bool, str]:
    """Check if the power unit is already assigned to another gateway"""
    if not isinstance(power_unit_id, int):
        error_msg = f"power_unit_id '{power_unit_id}' is not an integer so can't check if a gateway is already using it..."
        raise TypeError(error_msg)
        # return False, ""

    SQL = f"""
        select gateway
        from public.gw
        where power_unit_id = {power_unit_id}
    """
    _, rows = run_query(c, SQL, db="ijack", fetchall=True)

    if isinstance(rows, list) and len(rows) > 0:
        gateway = rows[0]["gateway"]
        c.logger.warning(
            f"power_unit_id '{power_unit_id}' is already in use by gateway '{gateway}'..."
        )
        return True, gateway

    return False, ""


def set_power_unit_to_gateway(c, power_unit_id_shadow: int, aws_thing: str) -> bool:
    """
    Update the public.gw record for 'aws_thing' to use the 'power_unit_id_shadow' from now on
    """
    if not isinstance(power_unit_id_shadow, int):
        error_msg = f"power_unit_id_shadow '{power_unit_id_shadow}' is not an integer, so not updating public.gw table for aws_thing '{aws_thing}'"
        raise TypeError(error_msg)

    if not isinstance(aws_thing, str) or not len(aws_thing) > 3:
        error_msg = f"aws_thing '{aws_thing}' is not a string or it's too short, so not updating public.gw table for power_unit_id_shadow '{power_unit_id_shadow}'"
        raise TypeError(error_msg)

    c.logger.warning(
        f"Updating public.gw aws_thing '{aws_thing}' record to use power_unit_id_shadow '{power_unit_id_shadow}'"
    )
    SQL = f"""
        update public.gw
        set power_unit_id = {power_unit_id_shadow}
        where aws_thing = '{aws_thing}'
    """
    _, rows = run_query(c, SQL, db="ijack", fetchall=False, commit=True)

    return True


def get_gateway_records(c, conn) -> list:
    """Get gateway records"""
    sql_gw = """
        select 
            distinct on (aws_thing)
            t1.id as gateway_id,
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
    return gw_rows


def get_power_unit_records(c, conn) -> list:
    """Get power_unit records"""
    sql_pu = """
        select 
            id as power_unit_id, 
            power_unit
        from public.power_units
    """
    _, pu_rows = run_query(c, sql_pu, db="ijack", fetchall=True, conn=conn)
    return pu_rows


def get_structure_records(c, conn) -> list:
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
    return structure_rows


def get_device_shadows_in_threadpool(c, gw_rows: list, client_iot) -> list:
    """Use concurrent.futures.ThreadPoolExecutor to efficiently gather all AWS IoT device shadows"""

    max_workers = 20
    n_gateways = len(gw_rows)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        c.logger.info(
            f"Gathering {n_gateways} gateways' AWS IoT device shadows in thread pool..."
        )

        futures = []
        for dict_ in gw_rows:
            aws_thing = dict_.get("aws_thing", None)
            future = executor.submit(get_iot_device_shadow, c, client_iot, aws_thing)
            futures.append(future)

        time1 = time.time()
        shadows = {}
        for future in as_completed(futures):
            try:
                data = future.result()
                aws_thing = data["aws_thing"]
            except Exception as exc:
                data = str(type(exc))
            finally:
                shadows[aws_thing] = data
                # print(str(len(shadows)), end="\r")

        time2 = time.time()

    n_shadows = len(shadows)
    # c.logger.info(pd.Series(shadows).value_counts())
    c.logger.info(
        f"'{n_shadows}' AWS IoT shadows collected out of '{n_gateways}' gateways in {(time2-time1)/60:.2f} minutes!"
    )

    return shadows


def get_shadow_table_html(c, shadow: dict) -> str:
    """Get an HTML table with all the info in the AWS IoT device shadow, for the email"""

    if not isinstance(shadow, dict) or not shadow:
        c.logger.error(
            f"ERROR: shadow '{shadow}' of type '{type(shadow)}' cannot be converted to html..."
        )
        return ""

    reported = shadow.get("state", {}).get("reported", {})
    reported_meta = shadow.get("metadata", {}).get("reported", {})

    # Get timestamps by reported dict key
    default_ts_if_not_found = time.time()
    reported_timestamps = {
        key: reported_meta.get(key, {}).get("timestamp", default_ts_if_not_found)
        for key in reported.keys()
    }
    # Sort by timestamp value, descending
    reported_timestamps_sorted = dict(
        sorted(reported_timestamps.items(), key=lambda item: item[1], reverse=True)
    )

    html = """
<table>
  <tr>
    <th>Item</th>
    <th>Value</th>
    <th>SK Time Updated</th>
  </tr>
    """

    counter = 0
    HEX_WHITE = "#FFFFFF"
    HEX_LIGHT_GRAY = "#D3D3D3"
    for key, timestamp_utc in reported_timestamps_sorted.items():
        value = reported.get(key, None)
        counter += 1
        background_color = HEX_WHITE if counter % 2 == 0 else HEX_LIGHT_GRAY

        try:
            dt = utc_timestamp_to_datetime_string(
                timestamp_utc,
                to_pytz_timezone=pytz.timezone("America/Regina"),
                format_string="%Y-%m-%d %H:%M",
            )
        except Exception:
            dt = ""

        html += f"""
        <tr style="background-color: {background_color};">
            <td>{key}</td>
            <td>{value}</td>
            <td>{dt}</td>
        </tr>
        """

    html += "\n</table>"

    return html


def upsert_gw_info(
    c: Config,
    gateway_id: int,
    aws_thing: str,
    shadow: dict,
    conn: psycopg2_connect,
) -> bool:
    """Update (or insert) the gateway-reported info from the shadow into the RDS database"""

    if not gateway_id or not aws_thing:
        return False

    seconds_since, msg = seconds_since_last_any_msg(c, shadow)

    days_since_reported = round(seconds_since / (60 * 60 * 24), 1)

    timestamp_utc_now = datetime.utcnow()
    timestamp_utc_last_reported = timestamp_utc_now - timedelta(
        days=days_since_reported
    )
    timestamp_utc_now_str = str(timestamp_utc_now)

    values_dict = {
        "gateway_id": gateway_id,
        "aws_thing": aws_thing,
        "timestamp_utc_updated": timestamp_utc_now_str,
        "days_since_reported": days_since_reported,
        "time_since_reported": msg,
        "timestamp_utc_last_reported": timestamp_utc_last_reported,
    }

    # These are all capitalized in the AWS IoT device shadow.
    # The key is the public.gw_info database column name.
    # The value is the AWS IoT device shadow name
    metrics_to_update = {
        "os_name": "OS_NAME",
        "os_pretty_name": "OS_PRETTY_NAME",
        "os_version": "OS_VERSION",
        "os_version_id": "OS_VERSION_ID",
        "os_release": "OS_RELEASE",
        "os_machine": "OS_MACHINE",
        "os_platform": "OS_PLATFORM",
        "os_python_version": "OS_PYTHON_VERSION",
        "modem_model": "MODEM_MODEL",
        "modem_firmware_rev": "MODEM_FIRMWARE_REV",
        "modem_drivers": "MODEM_DRIVERS",
        "sim_operator": "SIM_OPERATOR",
        "swv_canpy": "SWV_PYTHON",
        "swv_plc": "SWV",
        "gw_type_reported": "gateway_type",
    }
    reported = shadow.get("state", {}).get("reported", {})
    for db_col_name, shadow_name in metrics_to_update.items():
        value = reported.get(shadow_name, -1)
        if value != -1:
            # Must escape/double-up apostrophes when inserting into PostgreSQL
            value = str(value).replace("'", "''")
            values_dict[db_col_name] = value

    insert_str = ""
    values_str = ""
    set_str = ""
    for db_col_name, value in values_dict.items():
        comma = ","
        if insert_str == "":
            comma = ""
        insert_str += f"{comma} {db_col_name}"
        # The actual value is in the "values" dictionary
        values_str += f"{comma} %({db_col_name})s"
        set_str += f"{comma} {db_col_name}='{value}'"

    sql = f"""
        INSERT INTO public.gw_info
            ({insert_str})
            VALUES ({values_str})
            ON CONFLICT (gateway_id) DO UPDATE
                SET {set_str}
    """
    
    # # For debugging only
    # if aws_thing == "00:60:E0:86:4D:00":
    #     print("")

    run_query(
        c,
        sql,
        db="ijack",
        fetchall=False,
        # Need a new connection each time, in case the transaction fails?
        # Or just raise_error=True?
        conn=conn,
        commit=True,
        values_dict=values_dict,
        raise_error=True,
    )

    return True


def record_can_bus_cellular_test(
    c: Config, gateway_id: int, cellular_good: bool, can_bus_good: bool
) -> bool:
    """Record that the CAN bus and cellular have been tested and are working, or not!"""

    # sql_gw = SQL("""
    sql_gw = f"""
        update public.gw
        set test_cellular={'true' if cellular_good else 'false'},
            test_can_bus={'true' if can_bus_good else 'false'}
        where id = {gateway_id}
    """
    # """).format(cell_good=Literal(cellular_good), can_good=Literal(can_bus_good), gateway_id=Literal(gateway_id))
    run_query(c, sql_gw, db="ijack", fetchall=False, commit=True)

    if cellular_good:
        user_id_shop_auto = 788  # SHOP automated user
        network_id_sasktel = 1
        # sql_gw_tested = SQL("""
        sql_gw_tested = f"""
            insert into public.gw_tested_cellular
            (user_id, timestamp_utc, gateway_id, network_id)
            values ({user_id_shop_auto}, '{datetime.utcnow()}', {gateway_id}, {network_id_sasktel})
        """
        # """).format(
        #     user_id=Literal(user_id_shop_auto),
        #     timestamp_utc=Identifier(str(datetime.utcnow())),
        #     gateway_id=Literal(gateway_id),
        #     network_id=Literal(network_id_sasktel)
        # )
        run_query(c, sql_gw_tested, db="ijack", fetchall=False, commit=True)

    return True


@error_wrapper()
def main(c: Config, commit: bool = False):
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

    time_start = time.time()
    # Start a try/except/finally block so we close the database connection
    try:
        gw_rows = get_gateway_records(c, conn)
        pu_rows = get_power_unit_records(c, conn)
        pu_dict = {row["power_unit"]: row["power_unit_id"] for row in pu_rows}
        structure_rows = get_structure_records(c, conn)

        # Get the Boto3 AWS IoT client for updating the "thing shadow"
        client_iot = get_client_iot()
        shadows = get_device_shadows_in_threadpool(c, gw_rows, client_iot)

        # # Do you want to save the fixtures for testing?
        # fixtures_to_save = {
        #     # "gw_rows": gw_rows,
        #     # "pu_rows": pu_rows,
        #     # "structure_rows": structure_rows,
        #     "shadows": shadows,
        # }
        # for fixture_name, fixture in fixtures_to_save.items():
        #     save_fixture(fixture_obj=fixture, name_stem=fixture_name)

        for gw_dict in gw_rows:
            aws_thing = gw_dict.get("aws_thing", None)
            gateway_id = gw_dict.get("gateway_id", None)
            if aws_thing == '00:60:E0:72:66:13':
                print('This gateway has a new latitude and longitude from the device shadow')
            if aws_thing == '00:1D:48:31:6A:7A':
                print('This gateway has a new latitude and longitude from the device shadow')

            # This "if aws_thing is None" is unnecessary since the nulls are filtered out in the query,
            # and simply not allowed in the table, but it doesn't hurt
            if aws_thing is None:
                c.logger.warning(
                    '"AWS thing" is None. Continuing with next aws_thing in public.gw table...'
                )
                continue

            # shadow = get_iot_device_shadow(c, client_iot, aws_thing)
            shadow = shadows.get(aws_thing, {})
            if not shadow or not isinstance(shadow, dict):
                c.logger.warning(
                    f'No shadow exists for aws_thing "{aws_thing}". Continuing with next AWS_THING in public.gw table...'
                )
                continue

            # Update the public.gw_info table using info reported in the shadow
            upsert_gw_info(c, gateway_id, aws_thing, shadow, conn)

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
            power_unit_id_gw = gw_dict.get("power_unit_id", None)
            power_unit_gw = gw_dict.get("power_unit", None)

            # Compare the GPS first
            structure = None
            customer = None
            structure_rows_relevant = [
                row
                for row in structure_rows
                if row["power_unit_id"] == power_unit_id_gw
            ]
            for row in structure_rows_relevant:
                structure = row["structure"]
                customer = row["customer"]

                # GPS latitude
                if (
                    latitude_shadow and str(latitude_shadow)[:7] != "50.1631"
                ):  # IJACK SHOP GPS
                    compare_shadow_and_db(
                        c,
                        latitude_shadow,
                        row["gps_lat"],
                        "gps_lat",
                        power_unit_id_gw,
                        power_unit_shadow,
                        structure,
                        aws_thing,
                        commit=commit,
                    )

                # GPS longitude
                if (
                    longitude_shadow and str(longitude_shadow)[:7] != "101.675"
                ):  # IJACK SHOP GPS
                    compare_shadow_and_db(
                        c,
                        longitude_shadow,
                        row["gps_lon"],
                        "gps_lon",
                        power_unit_id_gw,
                        power_unit_shadow,
                        structure,
                        aws_thing,
                        commit=commit,
                    )

            if power_unit_id_shadow == power_unit_id_gw:
                c.logger.info(
                    f"Power unit '{power_unit_shadow}' in the public.gw table matches the one reported in the device shadow. Continuing with next..."
                )
                continue

            if aws_thing == "00:60:E0:86:4C:DA" and str(power_unit_shadow) == "200442":
                # Richie needs to fix this on on-site, so it uses the correct 200408 power unit
                continue

            if aws_thing == "00:60:E0:86:4C:DA" and date.today() < date(2022, 6, 30):
                c.logger.warning(
                    "skipping gateway '00:60:E0:86:4C:DA' since Richie needs to reset the power unit on the CAN bus, on-site..."
                )
                continue

            gateway_already_has_power_unit = bool(power_unit_id_gw)

            is_power_unit_in_use, gateway_already_linked = is_power_unit_already_in_use(
                c, power_unit_id_shadow
            )
            if is_power_unit_in_use:
                # There's a problem since another gateway is already using that power unit
                emailees_list = c.EMAIL_LIST_DEV
                subject = f"Power unit '{power_unit_shadow}' already used by gateway {gateway_already_linked}"
                html = f"Can't set gateway {aws_thing} power unit to {power_unit_shadow} because that power unit is already used by gateway {gateway_already_linked}"

                html += (
                    "\n<p><b>See which unit is already using that power unit:</b></p>"
                )
                html += "\n<ul>"
                html += f'\n<li><a href="https://myijack.com/rcom/?power_unit={power_unit_shadow}">https://myijack.com/rcom/?power_unit={power_unit_shadow}</a></li>'
                html += f'\n<li><a href="https://myijack.com/rcom/?gateway={gateway_already_linked}">https://myijack.com/rcom/?gateway={gateway_already_linked}</a></li>'
                html += f'\n<li><a href="https://us-west-2.console.aws.amazon.com/iot/home?region=us-west-2#/thing/{gateway_already_linked}/namedShadow/Classic%20Shadow">https://us-west-2.console.aws.amazon.com/iot/home?region=us-west-2#/thing/{gateway_already_linked}/namedShadow/Classic%20Shadow</a></li>'
                html += "\n</ul>"

                html += f"\n<p><b>New gateway that also wants to use power unit '{power_unit_shadow}':</b></p>"
                html += "\n<ul>"
                html += f'\n<li><a href="https://myijack.com/rcom/?gateway={aws_thing}">https://myijack.com/rcom/?gateway={aws_thing}</a></li>'
                html += f'\n<li><a href="https://us-west-2.console.aws.amazon.com/iot/home?region=us-west-2#/thing/{aws_thing}/namedShadow/Classic%20Shadow">https://us-west-2.console.aws.amazon.com/iot/home?region=us-west-2#/thing/{aws_thing}/namedShadow/Classic%20Shadow</a></li>'
                html += "\n</ul>"

            elif gateway_already_has_power_unit:
                # There's a problem since the gateway already has a power unit assigned to it
                emailees_list = c.EMAIL_LIST_DEV
                subject = f"Gateway {aws_thing} already linked to power unit {power_unit_gw} so can't link new power unit {power_unit_shadow}"
                html = f"Can't link gateway {aws_thing} to power unit {power_unit_shadow} because the gateway is already linked to power unit {power_unit_gw}"

                html += f"\n<p><b>See already-linked power unit '{power_unit_gw}' in action:</b></p>"
                html += "\n<ul>"
                html += f'\n<li><a href="https://myijack.com/rcom/?power_unit={power_unit_gw}">https://myijack.com/rcom/?power_unit={power_unit_gw}</a></li>'
                html += f'\n<li><a href="https://myijack.com/rcom/?gateway={aws_thing}">https://myijack.com/rcom/?gateway={aws_thing}</a></li>'
                html += f'\n<li><a href="https://us-west-2.console.aws.amazon.com/iot/home?region=us-west-2#/thing/{aws_thing}/namedShadow/Classic%20Shadow">https://us-west-2.console.aws.amazon.com/iot/home?region=us-west-2#/thing/{aws_thing}/namedShadow/Classic%20Shadow</a></li>'
                html += "\n</ul>"

            else:
                # No gateway is using that power unit, so link the two in the public.gw table
                set_power_unit_to_gateway(c, power_unit_id_shadow, aws_thing)
                emailees_list = c.EMAIL_LIST_SERVICE_PRODUCTION_IT
                subject = f"Power unit {power_unit_shadow} now linked to gateway {aws_thing}"
                html = f"<p>Power unit {power_unit_shadow} is now linked to gateway {aws_thing}."
                html += f' Check it out at <a href="https://myijack.com/rcom/?power_unit={power_unit_shadow}">https://myijack.com/rcom/?power_unit={power_unit_shadow}</a></p>'
                html += "\n<p>This gateway just noticed this new power unit on the CAN bus, and the power unit is not used by any other gateway.</p>"
                html += "\n<p>This gateway is also not already linked to an existing power unit.</p>"

                record_can_bus_cellular_test(
                    c, gateway_id, cellular_good=True, can_bus_good=True
                )

            if not structure:
                html += f"\n<p>There is no structure matched to power unit '{power_unit_shadow}'.</p>"
            else:
                html += f"\n<p>The structure for new current power unit '{power_unit_shadow}' is '{structure}'.</p>"

            if customer:
                html += f"\n<p>The customer for structure '{structure}' is '{customer}'.</p>"
            else:
                html += f"\n<p>There is no customer for power unit '{power_unit_shadow}'.</p>"

            html += f"\n<p><b>Edit the data in the 'Admin' site:</b></p>"
            html += "\n<ul>"
            html += f'\n<li>Structures table at <a href="https://myijack.com/admin/structures/?search={power_unit_shadow}">https://myijack.com/admin/structures/?search={power_unit_shadow}</a></li>'
            html += f'\n<li>Gateways table for <b><em>new</em></b> gateway "{aws_thing}" at <a href="https://myijack.com/admin/gateways/?search={aws_thing}">https://myijack.com/admin/gateways/?search={aws_thing}</a></li>'
            html += f'\n<li>Gateways table for <b><em>old</em></b> gateway "{gateway_already_linked}" at <a href="https://myijack.com/admin/gateways/?search={gateway_already_linked}">https://myijack.com/admin/gateways/?search={gateway_already_linked}</a></li>'
            html += "\n</ul>"

            shadow_html = get_shadow_table_html(c, shadow)
            if shadow_html:
                html += f"\n<p><b>AWS IoT device shadow data for new gateway '{aws_thing}':</b></p>"
                html += f"\n<p>{shadow_html}</p>"
            else:
                html += f"\n<p>No AWS IoT device shadow information for new gateway '{aws_thing}'.</p>"

            shadow_already_linked = shadows.get(gateway_already_linked, {})
            shadow_already_linked_html = get_shadow_table_html(c, shadow_already_linked)
            if shadow_already_linked_html:
                html += f"\n<p><b>AWS IoT device shadow data for previously-linked gateway '{gateway_already_linked}':</b></p>"
                html += f"\n<p>{shadow_already_linked_html}</p>"
            else:
                html += f"\n<p>No AWS IoT device shadow information for previously-linked gateway '{gateway_already_linked}'.</p>"

            c.logger.info(html)

            send_mailgun_email(
                c, text="", html=html, emailees_list=emailees_list, subject=subject
            )

        time_finish = time.time()
        c.logger.info(
            f"Time to update all gateways to their reported power units: {round(time_finish - time_start)} seconds"
        )

        del client_iot
    except Exception:
        raise
    finally:
        conn.close()

    return None


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c, commit=True)
