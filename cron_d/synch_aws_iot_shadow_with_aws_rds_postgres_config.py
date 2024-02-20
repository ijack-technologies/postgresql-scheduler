# import pandas as pd
import json
import logging
import pathlib
import sys
import time
from decimal import Decimal

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
    get_all_gateways,
    get_client_iot,
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

    # df = pd.DataFrame(rows, columns=columns)

    # Get the Boto3 AWS IoT client for updating the "thing shadow"
    client_iot = get_client_iot()

    rows: list = get_all_gateways(c)

    n_rows = len(rows)
    time_start = time.time()
    for counter, dict_ in enumerate(rows):
        # Initialize a new thing shadow for the data we're going to update in AWS IoT
        d = {"state": {"reported": {}}}

        # if dict_["gateway"] == "00:60:E0:84:A6:C7":
        #     # Just for debugging. Comment out if you don't need this
        #     print("found it")

        aws_thing = dict_["aws_thing"].upper()
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
                d["state"]["reported"][f"C__{key.upper()}"] = value.upper()
            else:
                d["state"]["reported"][f"C__{key.upper()}"] = value

        # Logger info
        customer = None
        try:
            # Get a slightly shorter customer name, if available
            customer = str(dict_["mqtt_topic"]).title()
        except Exception:
            c.logger.exception(
                "Trouble finding the MQTT topic. Is this the SHOP gateway? Continuing with the customer name instead..."
            )
            customer = dict_["customer"]

        c.logger.info(
            f"{counter + 1} of {n_rows}: Updating {customer} AWS_THING: {aws_thing}"
        )

        # Update the thing shadow for this gateway/AWS_THING
        try:
            json_payload_str: str = json.dumps(d)
            client_iot.update_thing_shadow(
                thingName=aws_thing, payload=json_payload_str
            )
        except TypeError:
            # If there's a problem with the JSON serialization, log the error and stop the program!
            c.logger.exception(
                "ERROR updating AWS IoT shadow for aws_thing '%s'", aws_thing
            )
            raise
        except Exception:
            c.logger.exception(
                "ERROR updating AWS IoT shadow for aws_thing '%s'", aws_thing
            )

    time_finish = time.time()
    c.logger.info(
        f"Time to update all AWS IoT thing shadows: {round(time_finish - time_start)} seconds"
    )

    return None


if __name__ == "__main__":
    main(c)
