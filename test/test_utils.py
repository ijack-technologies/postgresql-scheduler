# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.utils import Config, send_error_messages

LOGFILE_NAME = "test_time_series_update_views"

c = Config()
c.DEV_TEST_PRD = "development"


class TestAll(unittest.TestCase):
    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    @patch("project.utils.send_twilio_sms")
    @patch("project.utils.send_mailgun_email")
    def test_send_error_messages(
        self,
        mock_send_mailgun_email,
        mock_send_twilio_sms,
    ):
        """Test the send_error_messages() function"""

        global c
        err = Exception("This is an error message")
        filename = Path(__file__).name

        rv = send_error_messages(
            c=c, err=err, filename=filename, want_email=True, want_sms=True
        )

        self.assertIsNone(rv)
        mock_send_mailgun_email.assert_called_once()
        mock_send_twilio_sms.assert_called_once()


if __name__ == "__main__":
    unittest.main()
