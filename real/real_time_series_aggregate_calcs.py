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


from project import time_series_aggregate_calcs
from project.utils import Config


class TestAll(unittest.TestCase):
    @patch("project.time_series_aggregate_calcs.exit_if_already_running")
    def test_time_series_aggregate_calcs(self, mock_exit_if_already_running):
        """Test the main program"""

        c = Config()
        c.DEV_TEST_PRD = "development"
        # This c.TEST_FUNC just disables SMS, email, and phone call alerts
        c.TEST_FUNC = True
        LOGFILE_NAME = "real_time_series_aggregate_calcs"

        time_series_aggregate_calcs.main(c)

        mock_exit_if_already_running.assert_called_once()


if __name__ == "__main__":
    unittest.main()
