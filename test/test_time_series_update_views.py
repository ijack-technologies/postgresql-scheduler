# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.time_series_mv_refresh import (
    check_table_timestamps,
    force_refresh_continuous_aggregates,
    get_and_insert_latest_values,
    get_latest_timestamp_in_table,
)

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


# local imports
from project.utils import Config, configure_logging

LOGFILE_NAME = "test_time_series_update_views"

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

    # def test_gateways_mv_refresh(self):
    #     """Test the main program"""
    #     global c

    #     # This is no longer a materialized view--just a regular view
    #     with self.assertRaises(psycopg2.errors.WrongObjectType):
    #         gateways_mv_refresh.main(c)

    @patch("project.time_series_mv_refresh.run_query")
    def test_get_latest_timestamp_in_table(
        self,
        mock_run_query,
    ):
        """Test the get_latest_timestamp_in_table() function"""
        global c
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": datetime.utcnow()},
            ],
        )

        timestamp = get_latest_timestamp_in_table(c)

        assert isinstance(timestamp, datetime)
        assert timestamp < datetime.utcnow()
        mock_run_query.assert_called_once()

    @patch("project.time_series_mv_refresh.run_query")
    def test_get_latest_timestamp_in_table_raises_error(
        self,
        mock_run_query,
    ):
        """Test the get_latest_timestamp_in_table() function"""
        global c
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": None},
            ],
        )

        with self.assertRaises(ValueError):
            _: datetime = get_latest_timestamp_in_table(c)

        # The function should have been called 8 times
        self.assertEqual(mock_run_query.call_count, 8)

    @patch("project.time_series_mv_refresh.run_query")
    def test_get_latest_timestamp_in_table_threshold(
        self,
        mock_run_query,
    ):
        """Test the get_latest_timestamp_in_table() function where the timestamp is old"""
        global c
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": datetime.utcnow() - timedelta(hours=2)},
            ],
        )

        with self.assertRaises(Exception) as error:
            get_latest_timestamp_in_table(
                c, table="time_series_locf_copy", threshold=timedelta(hours=1)
            )

        the_exception = error.exception
        error_msg = the_exception.args[0]
        self.assertIn(
            "ERROR: latest timestamp in table 'time_series_locf_copy' is", error_msg
        )
        self.assertIn("which is before the threshold timedelta '1:00:00'", error_msg)

    @patch("project.time_series_mv_refresh.run_query")
    def test_check_table_timestamps_threshold(
        self,
        mock_run_query,
    ):
        """Test the check_table_timestamps() function where the timestamp is old"""
        global c

        # First test, where timestamp is recent enough (i.e. less than 1 hour old)
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": datetime.utcnow() - timedelta(hours=0.5)},
            ],
        )

        response = check_table_timestamps(
            c, tables=["time_series", "time_series_locf"], time_delta=timedelta(hours=1)
        )

        self.assertTrue(response)
        self.assertEqual(mock_run_query.call_count, 2)

        # Second test, where first table has an old timestamp > 1 hour old
        mock_run_query.reset_mock()
        mock_run_query.return_value = (
            ["timestamp_utc"],
            [
                {"timestamp_utc": datetime.utcnow() - timedelta(hours=2)},
            ],
        )

        with self.assertRaises(Exception) as error:
            response = check_table_timestamps(
                c,
                tables=["time_series", "time_series_locf"],
                time_delta=timedelta(hours=1),
            )

        the_exception = error.exception
        error_msg = the_exception.args[0]
        self.assertIn("ERROR: latest timestamp in table 'time_series' is", error_msg)
        self.assertIn("which is before the threshold timedelta '1:00:00'", error_msg)

        # The first table causes the error
        self.assertEqual(mock_run_query.call_count, 1)

    @patch("time.sleep")
    @patch(
        "project.time_series_mv_refresh.run_query",
        return_value=(
            ["power_unit", "power_unit_str", "gateway", "timestamp_utc", "signal"],
            [
                {
                    "power_unit": "1",
                    "power_unit_str": "1",
                    "gateway": "1",
                    "timestamp_utc": datetime.utcnow(),
                    "signal": 1,
                }
            ],
        ),
    )
    def test_get_and_insert_latest_values(
        self,
        mock_run_query,
        mock_sleep,
    ):
        """Test the get_and_insert_latest_values() function"""
        global c

        try:
            boolean = get_and_insert_latest_values(c, after_this_date=datetime.utcnow())
        except Exception as err:
            print(err)
            raise

        assert boolean is True
        self.assertTrue(mock_run_query.call_count, 4)

    @patch("project.time_series_mv_refresh.run_query")
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
