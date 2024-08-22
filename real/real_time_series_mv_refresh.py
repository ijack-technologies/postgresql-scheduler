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


from project import time_series_mv_refresh
from project.utils import Config, configure_logging

LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(__name__, logfile_name=LOGFILE_NAME)


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
        time_series_mv_refresh.main(c)


if __name__ == "__main__":
    unittest.main()
