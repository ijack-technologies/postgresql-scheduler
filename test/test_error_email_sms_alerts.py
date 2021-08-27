# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace/cron.d"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


import alarm_log_delete_duplicates

# import alarm_log_mv_refresh_old_non_surface
import alarm_log_mv_refresh
import gateways_mv_refresh
import time_series_mv_refresh

# local imports
from utils import Config, configure_logging

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace/cron.d"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


LOGFILE_NAME = "test_error_email_sms_alerts"

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

    def test_raise_error_email_time_series(self):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_FUNC = True
        c.TEST_ERROR = True
        with self.assertRaises(ValueError):
            time_series_mv_refresh.main(c)
        self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        self.assertEqual(c.TEST_DICT["rc"], "")
        self.assertEqual(c.TEST_DICT["message"], "")

    def test_raise_error_email_delete_duplicates(self):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_FUNC = True
        c.TEST_ERROR = True
        with self.assertRaises(ValueError):
            alarm_log_delete_duplicates.main(c)
        self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        self.assertEqual(c.TEST_DICT["rc"], "")
        self.assertEqual(c.TEST_DICT["message"], "")

    def test_raise_error_email_mv_refresh_new(self):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_FUNC = True
        c.TEST_ERROR = True
        with self.assertRaises(ValueError):
            alarm_log_mv_refresh.main(c)
        self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        self.assertEqual(c.TEST_DICT["rc"], "")
        self.assertEqual(c.TEST_DICT["message"], "")

    # def test_raise_error_email_mv_refresh_old(self):
    #     """Should send me an email and SMS if there's an error in the program"""
    #     global c
    #     c.TEST_FUNC = True
    #     c.TEST_ERROR = True
    #     with self.assertRaises(ValueError):
    #         alarm_log_mv_refresh_old_non_surface.main(c)
    #     self.assertIn('Sean, check ', c.TEST_DICT['msg_sms'])
    #     self.assertIn('There has been an error!', c.TEST_DICT['msg_sms'])
    #     self.assertEqual(c.TEST_DICT['rc'], '')
    #     self.assertEqual(c.TEST_DICT['message'], '')

    def test_raise_error_email_gateways_mv_refresh(self):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_FUNC = True
        c.TEST_ERROR = True
        with self.assertRaises(ValueError):
            gateways_mv_refresh.main(c)
        self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        self.assertEqual(c.TEST_DICT["rc"], "")
        self.assertEqual(c.TEST_DICT["message"], "")


if __name__ == "__main__":
    unittest.main()
