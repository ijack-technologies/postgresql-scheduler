import sys
import time
from pathlib import Path

import pytz
import schedule

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project import (
    alarm_log_delete_duplicates,
    alerts_bulk_processor,
    monitor_disk_space,  # type: ignore  # noqa: F401
    synch_aws_iot_shadow_with_aws_rds_postgres_config,
    time_series_aggregate_calcs,
    time_series_mv_refresh,
    time_series_rt_delete_old_data,
    timescaledb_restart_background_workers,
    update_fx_exchange_rates_daily,
    update_info_from_shadows,
    upload_bom_master_parts_to_db,
)
from project.logger_config import logger
from project.utils import Config


def make_schedule(c: Config) -> None:
    """Make a cron-like schedule for running tasks"""

    logger.info("Making the cron-like schedule...")

    schedule.every(30).minutes.do(time_series_mv_refresh.main, c=c)
    schedule.every(10).minutes.do(update_info_from_shadows.main, c=c, commit=True)

    schedule.every().hour.at(":03").do(
        synch_aws_iot_shadow_with_aws_rds_postgres_config.main, c=c
    )

    schedule.every().day.at("01:01", pytz.timezone("America/Regina")).do(
        alarm_log_delete_duplicates.main, c=c
    )
    schedule.every().day.at("01:11", pytz.timezone("America/Regina")).do(
        time_series_aggregate_calcs.main, c=c
    )
    schedule.every().day.at("01:21", pytz.timezone("America/Regina")).do(
        time_series_rt_delete_old_data.main, c=c
    )
    schedule.every().day.at("01:31", pytz.timezone("America/Regina")).do(
        upload_bom_master_parts_to_db.main, c=c
    )
    schedule.every().day.at("01:41", pytz.timezone("America/Regina")).do(
        timescaledb_restart_background_workers.main, c=c
    )
    schedule.every().day.at("01:51", pytz.timezone("America/Regina")).do(
        update_fx_exchange_rates_daily.main, c=c
    )
    schedule.every().day.at("02:01", pytz.timezone("America/Regina")).do(
        alerts_bulk_processor.main, c=c
    )

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
