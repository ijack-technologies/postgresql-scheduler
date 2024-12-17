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


from project import synch_aws_iot_shadow_with_aws_rds_postgres_config
from project.logger_config import configure_logging
from project.utils import Config

logger = logging.getLogger(__name__)


class TestAll(unittest.TestCase):
    @patch(
        "project.synch_aws_iot_shadow_with_aws_rds_postgres_config.exit_if_already_running"
    )
    def test_update_info_from_shadows(self, mock_exit_if_already_running):
        """Test the main program"""

        c = Config()
        c.DEV_TEST_PRD = "development"
        # This c.TEST_FUNC just disables SMS, email, and phone call alerts
        c.TEST_FUNC = True
        LOGFILE_NAME = "test_synch_aws_iot_shadow_with_aws_rds_postgres_config"
        configure_logging(__name__, logfile_name=LOGFILE_NAME)

        synch_aws_iot_shadow_with_aws_rds_postgres_config.main(c)


if __name__ == "__main__":
    unittest.main()
