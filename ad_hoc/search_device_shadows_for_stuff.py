# import pandas as pd
import json
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
    get_all_gateways,
    get_client_iot,
    run_query,
    get_iot_device_shadow,
)

c = Config()
c.logger = configure_logging(
    "search_shadows", "search_shadows.log", path_to_log_directory="/var/log/"
)

rows = get_all_gateways(c)

# Get the Boto3 AWS IoT client for updating the "thing shadow"
client_iot = get_client_iot()

search_for = "5328"
for i, dict_ in enumerate(rows):
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

    phone_country = reported.get("PHONE_COUNTRY", None)
    phone_area = reported.get("PHONE_AREA", None)
    phone_number = reported.get("PHONE_NUMBER", None)

    if phone_number and search_for in str(phone_number):
        print("got it!")

print("Finished")
