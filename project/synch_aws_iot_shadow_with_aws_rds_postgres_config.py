import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.logger_config import logger
from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    get_client_iot,
    run_query,
)


def update_device_shadows_in_threadpool(
    gateways_to_update: dict, client_iot: boto3.client
) -> list:
    """Use concurrent.futures.ThreadPoolExecutor to efficiently gather all AWS IoT device shadows"""

    max_workers = 20
    n_gateways = len(gateways_to_update)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        logger.info(
            f"Updating {n_gateways} gateways' AWS IoT device shadows in thread pool..."
        )

        future_to_aws_thing_dict = {}
        for aws_thing, json_payload_str in gateways_to_update.items():
            future_to_aws_thing_dict[
                executor.submit(
                    client_iot.update_thing_shadow,
                    thingName=aws_thing,
                    payload=json_payload_str,
                )
            ] = aws_thing

        time1 = time.time()
        success_dict = {}
        errors_dict = {}
        for index, future in enumerate(as_completed(future_to_aws_thing_dict)):
            aws_thing = future_to_aws_thing_dict[future]
            logger.info(f"{index + 1} of {n_gateways} shadows updated: {aws_thing}")
            status_code: int = None
            response_payload: str = None
            try:
                response: dict = future.result()
                streamingBody: StreamingBody = response["payload"]
                response_payload = json.loads(streamingBody.read())
                # HTTP status code
                status_code = response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode", None
                )
            except ClientError as err:
                # This is the most common error, when the AWS IoT thing doesn't exist.
                # It's always true but we shouldn't have asserts in production code.
                # assert err.response["Error"]["Code"] == "ResourceNotFoundException"
                status_code = 404
                response_payload = f"ResourceNotFoundException: {err}"
                errors_dict[aws_thing] = response_payload
            except Exception as exc:
                status_code = 500
                response_payload = str(type(exc))
                errors_dict[aws_thing] = response_payload
                raise
            else:
                success_dict[aws_thing] = {
                    "status": status_code,
                    "payload": response_payload,
                }
                # print(str(len(success_dict)), end="\r")

        time2 = time.time()

    n_success_dict = len(success_dict)
    logger.info(
        f"{n_success_dict} of {n_gateways} AWS IoT shadows successfully updated in {(time2 - time1) / 60:.1f} minutes!"
    )
    for aws_thing, response_payload in errors_dict.items():
        logger.error(
            f"Error updating AWS IoT shadow for {aws_thing}: {response_payload}"
        )

    return success_dict


def get_all_power_units_config_metrics() -> list:
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
                ) as modbus_networks,
                never_default as modbus_never_default,
                vpn_subnets as modbus_vpn_subnets,
                auto_vpn_routing as modbus_auto_vpn_routing
            from power_units_modbus_networks
            group by power_unit_id, never_default
        ),
        fixed_ip as (
            select 
                power_unit_id,
                STRING_AGG(
                    CONCAT(ip_address, '>', subnet, '>', gateway),
                    ','
                ) as fixed_ip_networks,
                never_default as fixed_ip_never_default
                -- vpn_subnets AS fixed_ip_vpn_subnets,
                -- auto_vpn_routing AS fixed_ip_auto_vpn_routing
            from power_units_fixed_ip_networks
            group by power_unit_id, never_default
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
            modbus.modbus_networks,
            modbus.modbus_never_default,
            modbus.modbus_vpn_subnets,
            modbus.modbus_auto_vpn_routing,
            fixed_ip.fixed_ip_networks,
            fixed_ip.fixed_ip_never_default
        FROM gw gw
        LEFT JOIN power_units pu ON gw.power_unit_id = pu.id
        LEFT JOIN structures str ON pu.id = str.power_unit_id
        LEFT JOIN public.structure_customer_rel str_cust_rel ON str_cust_rel.structure_id = str.id
        LEFT JOIN public.customers cust ON str_cust_rel.customer_id = cust.id
        LEFT JOIN public.structure_cust_sub_group_rel csr ON csr.structure_id = str.id
        LEFT JOIN public.cust_sub_groups cust_sub ON cust_sub.id = csr.cust_sub_group_id
        LEFT JOIN public.time_zones tz ON str.time_zone_id = tz.id
        LEFT JOIN public.model_types mt ON str.model_type_id = mt.id
        LEFT JOIN public.unit_types ut ON str.unit_type_id = ut.id
        LEFT JOIN modbus ON pu.id = modbus.power_unit_id
        LEFT JOIN fixed_ip ON pu.id = fixed_ip.power_unit_id
        where gw.aws_thing <> 'test'
            and gw.aws_thing is not null
            and cust.id is distinct from 21 -- demo customer
    """
    _, rows = run_query(SQL, db="ijack", fetchall=True)
    if not rows:
        raise ValueError("No rows found in the database for the power units!!!")

    return rows


@error_wrapper(filename=Path(__file__).name)
def main(c: Config) -> None:
    """
    Query unit data from AWS RDS "IJACK" PostgreSQL database,
    and update it in the AWS IoT "thing shadow" from which the gateways
    will update their local config data. This will be more robust than trying
    to connect to a PostgreSQL database over the internet (too many connections cause errors).
    The AWS IoT thing shadow is more robust, and AWS IoT can accept almost infinite connections at once.
    """

    exit_if_already_running(c, Path(__file__).name)

    # df = pd.DataFrame(rows, columns=columns)

    # Get the Boto3 AWS IoT client for updating the "thing shadow"
    client_iot: boto3.client = get_client_iot()

    # Get all gateways from database, and all the fields we're going
    # to update in the AWS IoT device shadow with C__{METRIC}
    rows: list = get_all_power_units_config_metrics()

    # Dict to which we'll add aws_thing: shadow pairs,
    # which we'll then update efficiently in a thread pool
    gateways_to_update: dict = {}

    # n_rows = len(rows)
    time_start = time.time()
    for dict_ in rows:
        # Logger info
        aws_thing = dict_["aws_thing"].upper()

        # customer = None
        # try:
        #     # Get a slightly shorter customer name, if available
        #     customer = str(dict_["mqtt_topic"]).title()
        # except Exception:
        #     logger.exception(
        #         "Trouble finding the MQTT topic. Is this the SHOP gateway? Continuing with the customer name instead..."
        #     )
        #     customer = dict_["customer"]
        # logger.info(
        #     f"Preparing {counter + 1} of {n_rows} for {customer} AWS_THING: {aws_thing}..."
        # )

        # Initialize a new thing shadow for the data we're going to update in AWS IoT
        shadow_new = {"state": {"reported": {}}}

        # if dict_["gateway"] == "00:60:E0:84:A7:15":
        #     # Just for debugging. Comment out if you don't need this
        #     print("found it")

        for key, value in dict_.items():
            # For debugging
            # if key == "ip_modbus" and aws_thing == "00:60:E0:84:A6:DB":
            #     print("")
            if value is None:
                # This way old values in the gateway's c.config dict, saved on the hard drive,
                # get overwritten if they used to have a value like "Calgary" and now they're null.
                # Otherwise they're just deleted from the device shadow and the gateway never sees them.
                value = ""
            # Convert Decimal types to floats for JSON serialization. Otherwise there will be an error!
            if isinstance(value, Decimal):
                value = float(value)
            if key in ("gateway", "unit_type", "aws_thing"):
                shadow_new["state"]["reported"][f"C__{key.upper()}"] = value.upper()
            else:
                shadow_new["state"]["reported"][f"C__{key.upper()}"] = value

        try:
            json_payload_str: str = json.dumps(shadow_new)
            gateways_to_update[aws_thing] = json_payload_str

            # Update the thing shadow for this gateway/AWS_THING
            # logger.info(
            #     f"{counter + 1} of {n_rows}: Updating {customer} AWS_THING: {aws_thing}"
            # )
            # client_iot.update_thing_shadow(
            #     thingName=aws_thing, payload=json_payload_str
            # )
        except TypeError:
            # If there's a problem with the JSON serialization, log the error and stop the program!
            logger.exception(
                "ERROR serializing JSON string for aws_thing '%s'", aws_thing
            )
            raise
        # except Exception:
        #     logger.exception(
        #         "ERROR updating AWS IoT shadow for aws_thing '%s'", aws_thing
        #     )

    update_device_shadows_in_threadpool(gateways_to_update, client_iot)

    time_finish = time.time()
    logger.info(
        f"Time to update all AWS IoT thing shadows: {round(time_finish - time_start)} seconds"
    )

    return None


if __name__ == "__main__":
    LOGFILE_NAME = "synch_aws_iot_shadow_with_aws_rds_postgres_config"
    c = Config()

    main(c)
