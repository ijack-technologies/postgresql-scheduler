# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from cron_d import synch_aws_iot_shadow_with_aws_rds_postgres_config
from cron_d.utils import Config, configure_logging


LOGFILE_NAME = "test_synch_aws_iot_shadow_with_aws_rds_postgres_config"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)


class TestAll(unittest.TestCase):
    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        # This c.TEST_FUNC just disables SMS, email, and phone call alerts
        c.TEST_FUNC = True

    @patch(
        "cron_d.synch_aws_iot_shadow_with_aws_rds_postgres_config.exit_if_already_running"
    )
    def test_update_gw_power_unit_id_from_shadow(self, mock_exit_if_already_running):
        """Test the main program"""
        global c
        synch_aws_iot_shadow_with_aws_rds_postgres_config.main(c)


if __name__ == "__main__":
    unittest.main()
