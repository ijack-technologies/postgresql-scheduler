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


from project import update_info_from_shadows
from project.logger_config import configure_logging
from project.utils import Config

LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"
configure_logging(__name__, logfile_name=LOGFILE_NAME)
logger = logging.getLogger(__name__)


class TestAll(unittest.TestCase):
    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        # This c.TEST_FUNC just disables SMS, email, and phone call alerts
        c.TEST_FUNC = False

    @patch("project.update_info_from_shadows.exit_if_already_running")
    def test_update_info_from_shadows(self, mock_exit_if_already_running):
        """Test the main program"""
        global c
        update_info_from_shadows.main(c, commit=True)


if __name__ == "__main__":
    unittest.main()
