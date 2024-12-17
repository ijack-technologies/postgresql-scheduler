# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import logging
import sys
import unittest
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project import timescaledb_restart_background_workers
from project.logger_config import configure_logging
from project.utils import Config

LOGFILE_NAME = "really_run_timescaledb_restart_background_workers"

c = Config()
c.DEV_TEST_PRD = "development"
configure_logging(__name__, logfile_name=LOGFILE_NAME)
logger = logging.getLogger(__name__)


class TestAll(unittest.TestCase):
    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = False

    @patch("project.utils.find_pids")
    def test_timescaledb_restart_background_workers(self, mock_find_pids):
        """Really run the timescaledb_restart_background_workers.py file"""
        global c

        mock_find_pids.return_value = []

        is_good = timescaledb_restart_background_workers.main(c=c)

        self.assertTrue(is_good)


if __name__ == "__main__":
    unittest.main()
