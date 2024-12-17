# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project import time_series_mv_refresh
from project.utils import Config

LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"


class TestAll(unittest.TestCase):
    def setUp(self):
        """Executed prior to each test below, not just when the class is initialized"""
        global c
        c.DEV_TEST_PRD = "development"
        # This c.TEST_FUNC just disables SMS, email, and phone call alerts
        c.TEST_FUNC = True

    @patch("time.sleep", return_value=None)
    @patch("project.time_series_mv_refresh.exit_if_already_running")
    def test_time_series_mv_refresh(
        self, mock_exit_if_already_running, mock_time_sleep
    ):
        """Test the main program"""
        global c
        time_series_mv_refresh.main(c, by_power_unit=True)


if __name__ == "__main__":
    unittest.main()
