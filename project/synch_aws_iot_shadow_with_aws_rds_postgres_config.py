# import pandas as pd
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

# local imports
from project.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    get_all_gateways_config_metrics,
    get_client_iot,
)



def update_device_shadows_in_threadpool(
    c: Config, gateways_to_update: dict, client_iot: boto3.client
) -> list:
    """Use concurrent.futures.ThreadPoolExecutor to efficiently gather all AWS IoT device shadows"""

    max_workers = 20
    n_gateways = len(gateways_to_update)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        c.logger.info(
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
            c.logger.info(f"{index + 1} of {n_gateways} shadows updated: {aws_thing}")
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
    c.logger.info(
        f"{n_success_dict} of {n_gateways} AWS IoT shadows successfully updated in {(time2-time1)/60:.1f} minutes!"
    )
    for aws_thing, response_payload in errors_dict.items():
        c.logger.error(
            f"Error updating AWS IoT shadow for {aws_thing}: {response_payload}"
        )

    return success_dict


@error_wrapper()
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
    rows: list = get_all_gateways_config_metrics(c)

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
        #     c.logger.exception(
        #         "Trouble finding the MQTT topic. Is this the SHOP gateway? Continuing with the customer name instead..."
        #     )
        #     customer = dict_["customer"]
        # c.logger.info(
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
            # c.logger.info(
            #     f"{counter + 1} of {n_rows}: Updating {customer} AWS_THING: {aws_thing}"
            # )
            # client_iot.update_thing_shadow(
            #     thingName=aws_thing, payload=json_payload_str
            # )
        except TypeError:
            # If there's a problem with the JSON serialization, log the error and stop the program!
            c.logger.exception(
                "ERROR serializing JSON string for aws_thing '%s'", aws_thing
            )
            raise
        # except Exception:
        #     c.logger.exception(
        #         "ERROR updating AWS IoT shadow for aws_thing '%s'", aws_thing
        #     )

    update_device_shadows_in_threadpool(c, gateways_to_update, client_iot)

    time_finish = time.time()
    c.logger.info(
        f"Time to update all AWS IoT thing shadows: {round(time_finish - time_start)} seconds"
    )

    return None


if __name__ == "__main__":
    LOGFILE_NAME = "synch_aws_iot_shadow_with_aws_rds_postgres_config"
    c = Config()
    c.logger = configure_logging(__name__, logfile_name=LOGFILE_NAME)
    main(c)
