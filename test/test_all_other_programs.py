# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from cron_d import alarm_log_delete_duplicates

# from cron_d import alarm_log_mv_refresh_old_non_surface
from cron_d import alarm_log_mv_refresh
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

    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    def test_time_series(self):
        """Test the main program"""
        global c
        time_series_mv_refresh.main(c)

    # def test_alarm_log_refresh_old(self):
    #     """Test the main program"""
    #     global c
    #     alarm_log_mv_refresh_old_non_surface.main(c)

    def test_alarm_log_refresh_new(self):
        """Test the main program"""
        global c
        alarm_log_mv_refresh.main(c)

    def test_alarm_log_delete_duplicates(self):
        """Test the main program"""
        global c
        alarm_log_delete_duplicates.main(c)


if __name__ == "__main__":
    unittest.main()
