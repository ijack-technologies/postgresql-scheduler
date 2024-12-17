import logging
import time

import alarm_log_delete_duplicates
import schedule
import synch_aws_iot_shadow_with_aws_rds_postgres_config
import time_series_aggregate_calcs
import time_series_mv_refresh
import time_series_rt_delete_old_data
import timescaledb_restart_background_workers
import update_info_from_shadows
import upload_bom_master_parts_to_db
from logger_config import configure_logging
from utils import Config

configure_logging(__name__, logfile_name="main_scheduler")

logger = logging.getLogger(__name__)


def make_schedule(c: Config) -> None:
    """
    Make a cron-like schedule for running tasks

    # min hour dom month dow   command
    # */15 * * * * python3 /project/_archive/db_remove_old_connections.py
    # */3 * * * * python3 /project/_archive/gateways_mv_refresh.py
    # Delete duplicate alarm log records once daily
    1 1 * * * python3 /project/alarm_log_delete_duplicates.py
    # Recalculate aggregated time series records once daily
    11 1 * * * python3 /project/time_series_aggregate_calcs.py
    */30 * * * * python3 /project/time_series_mv_refresh.py
    31 1 * * * python3 /project/timescaledb_restart_background_workers.py
    3 * * * * python3 /project/synch_aws_iot_shadow_with_aws_rds_postgres_config.py
    */10 * * * * python3 /project/update_info_from_shadows.py
    """
    logger.info("Making the cron-like schedule...")
    schedule.every().day.at("01:01").do(alarm_log_delete_duplicates.main, c=c)
    schedule.every().day.at("01:11").do(time_series_aggregate_calcs.main, c=c)
    schedule.every().day.at("01:21").do(time_series_rt_delete_old_data.main, c=c)
    schedule.every().day.at("01:31").do(upload_bom_master_parts_to_db.main, c=c)
    schedule.every(30).minutes.do(time_series_mv_refresh.main, c=c)
    schedule.every().day.at("01:41").do(
        timescaledb_restart_background_workers.main, c=c
    )
    schedule.every().hour.at(":03").do(
        synch_aws_iot_shadow_with_aws_rds_postgres_config.main, c=c
    )
    schedule.every(10).minutes.do(update_info_from_shadows.main, c=c, commit=True)

    return None


def run_schedule() -> None:
    """
    Run the schedule of tasks and wait X seconds before running the scheduled tasks again.
    This loop will run forever.
    """

    # Configure the logger
    c = Config()

    # Make the schedule
    make_schedule(c=c)

    logger.info("App running ✅. Running scheduled tasks forever...")
    while True:
        # Run all scheduled tasks
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    run_schedule()
