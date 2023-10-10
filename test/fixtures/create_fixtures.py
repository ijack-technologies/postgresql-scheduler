import logging
import pathlib
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
    get_client_iot,
    get_conn,
)
from cron_d.update_gw_power_unit_id_from_shadow import (
    get_gateway_records,
    get_power_unit_records,
    get_structure_records,
    get_device_shadows_in_threadpool,
)
from test.fixtures.fixture_utils import save_fixture

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "update_gw_power_unit_id_from_shadow"


c = Config()
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)


conn = get_conn(c)
gw_rows = get_gateway_records(c, conn)
pu_rows = get_power_unit_records(c, conn)
pu_dict = {row["power_unit"]: row["power_unit_id"] for row in pu_rows}
structure_rows = get_structure_records(c, conn)

# Get the Boto3 AWS IoT client for updating the "thing shadow"
client_iot = get_client_iot()
shadows = get_device_shadows_in_threadpool(c, gw_rows, client_iot)

# Do you want to save the fixtures for testing?
fixtures_to_save = {
    "gw_rows": gw_rows,
    "pu_rows": pu_rows,
    "structure_rows": structure_rows,
    "shadows": shadows,
}
for name, fixture in fixtures_to_save.items():
    save_fixture(fixture, name)

print("\n\nDone")
