# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project._archive import db_remove_old_connections

# local imports
from project.utils import Config, configure_logging

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


LOGFILE_NAME = "test_db_remove_old_connections"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(__name__, logfile_name=LOGFILE_NAME)


class TestAll(unittest.TestCase):
    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    @patch("project._archive.db_remove_old_connections.run_query")
    @patch("project._archive.db_remove_old_connections.exit_if_already_running")
    def test_gateways_mv_refresh(self, mock_exit_if_already_running, mock_run_query):
        """Test the main program"""
        global c

        # This is no longer a materialized view--just a regular view
        db_remove_old_connections.main(c)

        mock_exit_if_already_running.assert_called_once()
        mock_run_query.assert_called_once()


if __name__ == "__main__":
    unittest.main()
