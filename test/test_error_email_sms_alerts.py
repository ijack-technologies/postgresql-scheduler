# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

from datetime import datetime
import sys
import unittest
from unittest.mock import MagicMock, patch


# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from cron_d import alarm_log_delete_duplicates

# import alarm_log_mv_refresh_old_non_surface
from cron_d import alarm_log_mv_refresh
from cron_d import gateways_mv_refresh
from cron_d import time_series_mv_refresh

# local imports
from cron_d.utils import Config, configure_logging

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
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
        c.TEST_DICT = {}

    @patch("cron_d.utils.send_twilio_sms")
    @patch("cron_d.utils.send_mailgun_email")
    @patch("cron_d.utils.check_if_c_in_args")
    def test_raise_error_email_time_series(self, mock_check, mock_mail, mock_twil):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_FUNC = True
        # c.TEST_ERROR = True
        mock_check.side_effect = Exception("server closed the connection unexpectedly")
        with self.assertRaises(Exception):
            time_series_mv_refresh.main(c)
        mock_check.assert_called_once()
        mock_mail.assert_called_once()
        mock_twil.assert_called_once()
        # self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        # self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        # self.assertEqual(c.TEST_DICT["rc"], "")
        # self.assertEqual(c.TEST_DICT["message"], "")

    @patch("cron_d.utils.send_twilio_sms")
    @patch("cron_d.utils.send_mailgun_email")
    @patch("cron_d.utils.check_if_c_in_args")
    def test_raise_error_email_delete_duplicates(
        self, mock_check, mock_mail, mock_twil
    ):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_FUNC = True
        # c.TEST_ERROR = True
        mock_check.side_effect = Exception("server closed the connection unexpectedly")
        with self.assertRaises(Exception):
            alarm_log_delete_duplicates.main(c)
        mock_check.assert_called_once()
        mock_mail.assert_called_once()
        mock_twil.assert_called_once()
        # self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        # self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        # self.assertEqual(c.TEST_DICT["rc"], "")
        # self.assertEqual(c.TEST_DICT["message"], "")

    @patch("cron_d.utils.send_twilio_sms")
    @patch("cron_d.utils.send_mailgun_email")
    @patch("cron_d.utils.check_if_c_in_args")
    def test_raise_error_email_mv_refresh_new(self, mock_check, mock_mail, mock_twil):
        """Should send me an email and SMS if there's an error in the program"""
        global c
        c.TEST_DICT = {}
        c.TEST_FUNC = True
        # c.TEST_ERROR = False
        mock_check.side_effect = Exception("server closed the connection unexpectedly")
        with self.assertRaises(Exception):
            alarm_log_mv_refresh.main(c)
        mock_check.assert_called_once()
        mock_mail.assert_called_once()
        mock_twil.assert_called_once()
        # self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
        # self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
        # self.assertEqual(c.TEST_DICT["rc"], "")
        # self.assertEqual(c.TEST_DICT["message"], "")

    # @patch("cron_d.utils.check_if_c_in_args")
    # def test_raise_error_email_mv_refresh_old(self, mock_check):
    #     """Should send me an email and SMS if there's an error in the program"""
    #     global c
    #     c.TEST_FUNC = True
    #     c.TEST_ERROR = True
    # mock_check.side_effect = Exception("server closed the connection unexpectedly")
    #     with self.assertRaises(Exception):
    #         alarm_log_mv_refresh_old_non_surface.main(c)
    # mock_check.assert_called_once()
    #     self.assertIn('Sean, check ', c.TEST_DICT['msg_sms'])
    #     self.assertIn('There has been an error!', c.TEST_DICT['msg_sms'])
    #     self.assertEqual(c.TEST_DICT['rc'], '')
    #     self.assertEqual(c.TEST_DICT['message'], '')

    # @patch("cron_d.utils.check_if_c_in_args")
    # def test_raise_error_email_gateways_mv_refresh(self, mock_check):
    #     """Should send me an email and SMS if there's an error in the program"""
    #     global c
    #     c.TEST_FUNC = True
    #     c.TEST_ERROR = True
    # mock_check.side_effect = Exception("server closed the connection unexpectedly")
    #     with self.assertRaises(Exception):
    #         gateways_mv_refresh.main(c)
    # mock_check.assert_called_once()
    #     self.assertIn("Sean, check ", c.TEST_DICT["msg_sms"])
    #     self.assertIn("There has been an error!", c.TEST_DICT["msg_sms"])
    #     self.assertEqual(c.TEST_DICT["rc"], "")
    #     self.assertEqual(c.TEST_DICT["message"], "")

    @patch("cron_d.utils.send_twilio_sms")
    @patch("cron_d.utils.send_mailgun_email")
    @patch("cron_d.utils.check_if_c_in_args")
    @patch("cron_d.utils.datetime.datetime")
    def test_its_8_am_UTC_time_error_every_night(
        self, mock_dt, mock_check, mock_mail, mock_twil
    ):
        """Test we ignore the 2-3am local time (8:00 UTC time) error every night"""

        # instance = mock_dt.return_value
        # mock_dt = MagicMock()
        # mock_dt
        mock_check.side_effect = Exception("server closed the connection unexpectedly")
        mock_dt.utcnow.return_value = datetime(2021, 1, 1, 8, 1)
        global c
        c.TEST_FUNC = True
        # c.TEST_ERROR = False
        alarm_log_mv_refresh.main(c)
        mock_check.assert_called_once()
        mock_mail.assert_not_called()
        mock_twil.assert_not_called()
        # No error is raised!
        mock_dt.utcnow.assert_called_once()


if __name__ == "__main__":
    unittest.main()
