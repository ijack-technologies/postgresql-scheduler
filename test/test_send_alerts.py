# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from unittest.mock import patch
from types import SimpleNamespace

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


# local imports
from project.utils import Config, configure_logging, send_mailgun_email, send_twilio_sms
from test.utils import create_mock_twilio_client


LOGFILE_NAME = "test_send_alerts"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/project/logs/"
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

    @patch("project.utils.Client")
    def test_twilio(self, mock_twilio_client):
        """Test if Twilio works"""
        global c

        warning = "Sean, this is just a test warning..."
        sms_phone_list = c.PHONE_LIST_DEV
        c.TEST_FUNC = False

        twilio_client_instance = create_mock_twilio_client()
        mock_twilio_client.return_value = twilio_client_instance

        message = send_twilio_sms(c, sms_phone_list, warning)

        mock_twilio_client.assert_called_once()
        c.logger.info(f"Twilio rc 'message': {message}")
        self.assertNotEqual(message, "")
        self.assertNotEqual(message.error_code, "None")
        self.assertNotEqual(message.error_message, "None")
        self.assertIn(warning, message.body)
        self.assertIn(
            "Reply STOP to unsubscribe from ALL IJACK SMS alerts", message.body
        )
        self.assertEqual(message.status, "queued")

    @patch("requests.post", return_value=SimpleNamespace(status_code=200))
    def test_mailgun_text_only(self, mock_post):
        """Test if mailgun text-only email works"""
        global c
        c.TEST_FUNC = False
        text = "Just testing whether Mailgun text email works"

        rc = send_mailgun_email(
            c,
            text=text,
            html="",
            emailees_list=c.EMAIL_LIST_DEV,
            subject="TEST - IJACK Alert - Text Only",
        )

        mock_post.assert_called_once()
        c.logger.info(f"Mailgun 'rc' for text email: {rc}")
        self.assertEqual(rc.status_code, 200)

    @patch("requests.post", return_value=SimpleNamespace(status_code=200))
    def test_mailgun_html_only(self, mock_post):
        """Test if mailgun html-only email works"""
        global c
        c.TEST_FUNC = False
        html = """<html><body><p>
                Customer: Whitecap Resources<br>
                Model: XFER 2270<br>
                Location: Gull Lake<br><br>
                Power unit: 200000<br><br></p>
                </body></html>"""

        rc = send_mailgun_email(
            c,
            text="",
            html=html,
            emailees_list=c.EMAIL_LIST_DEV,
            subject="TEST - IJACK Alert - HTML",
        )

        mock_post.assert_called_once()
        c.logger.info(f"Mailgun 'rc' for html email: {rc}")
        self.assertEqual(rc.status_code, 200)


if __name__ == "__main__":
    unittest.main()
