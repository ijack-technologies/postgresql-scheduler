# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

from datetime import datetime
import sys
import unittest
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from cron_d.time_series_mv_refresh import (
    force_refresh_continuous_aggregates,
    get_and_insert_latest_values,
    get_latest_timestamp_in_locf_copy,
)

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


# local imports
from cron_d.utils import Config, configure_logging

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


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

    # def test_gateways_mv_refresh(self):
    #     """Test the main program"""
    #     global c

    #     # This is no longer a materialized view--just a regular view
    #     with self.assertRaises(psycopg2.errors.WrongObjectType):
    #         gateways_mv_refresh.main(c)

    @patch("cron_d.time_series_mv_refresh.run_query")
    def test_get_latest_timestamp_in_locf_copy(
        self,
        mock_run_query,
    ):
        """Test the get_latest_timestamp_in_locf_copy() function"""
        global c
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": datetime.utcnow()},
            ],
        )

        timestamp = get_latest_timestamp_in_locf_copy(c)

        assert isinstance(timestamp, datetime)
        assert timestamp < datetime.utcnow()
        mock_run_query.assert_called_once()

    @patch("cron_d.time_series_mv_refresh.run_query")
    def test_get_latest_timestamp_in_locf_copy_raises_error(
        self,
        mock_run_query,
    ):
        """Test the get_latest_timestamp_in_locf_copy() function"""
        global c
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": None},
            ],
        )

        with self.assertRaises(ValueError):
            get_latest_timestamp_in_locf_copy(c)

        mock_run_query.assert_called_once()

    @patch("cron_d.time_series_mv_refresh.run_query")
    def test_get_and_insert_latest_values(
        self,
        mock_run_query,
    ):
        """Test the get_and_insert_latest_values() function"""
        global c

        boolean = get_and_insert_latest_values(c, after_this_date=datetime.utcnow())
        assert boolean is True
        mock_run_query.assert_called_once()

    @patch("cron_d.time_series_mv_refresh.run_query")
    def test_force_refresh_continuous_aggregates(
        self,
        mock_run_query,
    ):
        """Test the force_refresh_continuous_aggregates() function"""
        global c

        boolean = force_refresh_continuous_aggregates(
            c, after_this_date=datetime.utcnow()
        )
        assert boolean is True
        assert mock_run_query.call_count == 5


if __name__ == "__main__":
    unittest.main()
