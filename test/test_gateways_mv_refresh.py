# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
import psycopg2

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace/cron.d"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

# local imports
from utils import Config, configure_logging
import gateways_mv_refresh

# import alarm_log_mv_refresh_old_non_surface

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

    def test_gateways_mv_refresh(self):
        """Test the main program"""
        global c

        # This is no longer a materialized view--just a regular view
        with self.assertRaises(psycopg2.errors.WrongObjectType):
            gateways_mv_refresh.main(c)


if __name__ == "__main__":
    unittest.main()
