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

from project import timescaledb_restart_background_workers

# local imports
from project.utils import Config
from project.logger_config import configure_logging

LOGFILE_NAME = "test_timescaledb_restart_background_workers"

c = Config()
c.DEV_TEST_PRD = "development"
configure_logging(__name__, logfile_name=LOGFILE_NAME)


class TestAll(unittest.TestCase):
    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = False

    @patch("project.utils.find_pids")
    @patch("project.timescaledb_restart_background_workers.run_query")
    def test_timescaledb_restart_background_workers(
        self, mock_run_query, mock_find_pids
    ):
        """Test the timescaledb_restart_background_workers.py file"""
        global c

        mock_find_pids.return_value = []

        is_good = timescaledb_restart_background_workers.main(c=c)

        self.assertTrue(is_good)
        self.assertEqual(mock_run_query.call_count, 1)


if __name__ == "__main__":
    unittest.main()
