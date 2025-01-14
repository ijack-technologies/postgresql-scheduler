import sys
from pathlib import Path

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = str(Path(__file__).parent.parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.update_info_from_shadows import (
    get_device_shadows_in_threadpool,
    get_gateway_records,
)
from project.utils import Config, get_client_iot, get_conn
from test.fixtures.fixture_utils import save_fixture

LOGFILE_NAME = "update_info_from_shadows"


c = Config()


with get_conn(db="aws_rds") as conn:
    gw_rows = get_gateway_records()
    GING_GATEWAY = "00:60:E0:84:A7:15"
    gw_rows = [row for row in gw_rows if row["aws_thing"] == GING_GATEWAY]
    gw_rows[0]["gps_lat"] = 51.0
    gw_rows[0]["gps_lon"] = -114.0
    gw_rows[0]["hours"] = 99.9
    # pu_rows = get_power_unit_records(c, conn)
    # # pu_dict = {row["power_unit"]: row["power_unit_id"] for row in pu_rows}
    # structure_rows = get_structure_records(c, conn)

    # Get the Boto3 AWS IoT client for updating the "thing shadow"
    client_iot = get_client_iot()
    shadows = get_device_shadows_in_threadpool(c, gw_rows, client_iot)
    # The hours just passed 100
    shadows[GING_GATEWAY]["state"]["reported"]["HOURS"] = 100.1
    # The GPS location is now 51.1, -114.1
    shadows[GING_GATEWAY]["state"]["reported"]["LATITUDE"] = 51.1
    shadows[GING_GATEWAY]["state"]["reported"]["LONGITUDE"] = -114.1
    # The serial number is now 200999
    # shadows[GING_GATEWAY]["state"]["reported"]["SERIAL_NUMBER"] = 200999

    # Do you want to save the fixtures for testing?
    fixtures_to_save = {
        "gw_rows": gw_rows,
        # "pu_rows": pu_rows,
        # "structure_rows": structure_rows,
        "shadows": shadows,
    }
    for name, fixture in fixtures_to_save.items():
        save_fixture(fixture, name)

print("\n\nDone")
