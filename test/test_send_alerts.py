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


# local imports
from utils import Config, configure_logging, send_mailgun_email, send_twilio_sms

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace/cron.d"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


LOGFILE_NAME = "test_send_alerts"

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

    def test_twilio(self):
        """Test if Twilio works"""
        global c

        warning = "Sean, this is just a test warning..."
        sms_phone_list = c.PHONE_LIST_DEV
        c.TEST_FUNC = False

        message = send_twilio_sms(c, sms_phone_list, warning)
        c.logger.info(f"Twilio rc 'message': {message}")
        self.assertNotEqual(message, "")
        self.assertNotEqual(message.error_code, "None")
        self.assertNotEqual(message.error_message, "None")
        self.assertIn(warning, message.body)
        self.assertIn("Reply STOP to unsubscribe from ALL IJACK SMS alerts", message.body)
        self.assertEqual(message.status, "queued")

    def test_mailgun_text_only(self):
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
        c.logger.info(f"Mailgun 'rc' for text email: {rc}")
        self.assertEqual(rc.status_code, 200)

    def test_mailgun_html_only(self):
        """Test if mailgun html-only email works"""
        global c
        c.TEST_FUNC = False
        html = f"""<html><body><p>
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
        c.logger.info(f"Mailgun 'rc' for html email: {rc}")
        self.assertEqual(rc.status_code, 200)


if __name__ == "__main__":
    unittest.main()
