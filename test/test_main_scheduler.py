import logging
import sys
import time
import unittest

import schedule

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.logger_config import configure_logging
from project.utils import Config

LOGFILE_NAME = "test_main_scheduler"

c = Config()
c.DEV_TEST_PRD = "development"
configure_logging(__name__, logfile_name=LOGFILE_NAME)

logger = logging.getLogger(__name__)


class TestAll(unittest.TestCase):
    """Test all functions"""

    # @patch("time.sleep", return_value=None)
    # @patch("project.time_series_mv_refresh.get_and_insert_latest_values")
    # @patch("project.time_series_mv_refresh.force_refresh_continuous_aggregates")
    # @patch("project.time_series_mv_refresh.check_table_timestamps")
    # @patch("project.time_series_mv_refresh.get_latest_timestamp_in_table")
    # @patch("project.time_series_mv_refresh.exit_if_already_running")
    def test_time_series_mv_refresh(
        self,
        # mock_exit_if_already_running,
        # mock_get_latest_timestamp_in_table,
        # mock_check_table_timestamps,
        # mock_force_refresh_continuous_aggregates,
        # mock_get_and_insert_latest_values,
        # mock_time_sleep
    ):
        """Test the main program"""
        global c

        c.test_var = False

        def slow_func(c: Config) -> None:
            """Sets test_var to True"""
            logger.info("Slow function running...")
            c.test_var = True

        schedule.every(1).seconds.do(slow_func, c=c)

        time_start = time.time()
        time_finish = time.time() + 0.01
        while True:
            # schedule.run_pending()
            schedule.run_all()

            if time.time() > time_finish:
                break
        seconds_taken = time.time() - time_start
        print(f"Seconds taken: {seconds_taken}")

        self.assertTrue(c.test_var)
        # self.assertTrue(mock_exit_if_already_running.call_count > 0)
        # self.assertTrue(mock_get_latest_timestamp_in_table.call_count > 0)
        # self.assertTrue(mock_check_table_timestamps.call_count > 0)
        # self.assertTrue(mock_force_refresh_continuous_aggregates.call_count > 0)
        # self.assertTrue(mock_get_and_insert_latest_values.call_count > 0)

        # mock_time_sleep.assert_called()


if __name__ == "__main__":
    unittest.main()
