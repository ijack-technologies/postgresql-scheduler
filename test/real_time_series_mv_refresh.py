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


# import alarm_log_mv_refresh_old_non_surface
from cron_d import time_series_mv_refresh

# local imports
from cron_d.utils import Config, configure_logging

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)


class TestAll(unittest.TestCase):

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        # This c.TEST_FUNC just disables SMS, email, and phone call alerts
        c.TEST_FUNC = True

    @patch("cron_d.time_series_mv_refresh.exit_if_already_running")
    def test_time_series_mv_refresh(self, mock_exit_if_already_running):
        """Test the main program"""
        global c
        time_series_mv_refresh.main(c)


if __name__ == "__main__":
    unittest.main()
