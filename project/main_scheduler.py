import time

# # Insert pythonpath into the front of the PATH environment variable, before importing anything
# import sys
# from pathlib import Path
# pythonpath = str(Path(__file__).parent)
# try:
#     sys.path.index(pythonpath)
# except ValueError:
#     sys.path.insert(0, pythonpath)
import alarm_log_delete_duplicates
import time_series_rt_delete_old_data
import schedule
import synch_aws_iot_shadow_with_aws_rds_postgres_config
import time_series_aggregate_calcs
import time_series_mv_refresh
import timescaledb_restart_background_workers
import update_info_from_shadows
from utils import Config, configure_logging


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
    c.logger.info("Making the cron-like schedule...")
    schedule.every().day.at("01:01").do(alarm_log_delete_duplicates.main, c=c)
    schedule.every().day.at("01:11").do(time_series_aggregate_calcs.main, c=c)
    schedule.every().day.at("01:21").do(time_series_rt_delete_old_data.main, c=c)
    schedule.every(30).minutes.do(time_series_mv_refresh.main, c=c)
    schedule.every().day.at("01:31").do(
        timescaledb_restart_background_workers.main, c=c
    )
    schedule.every().hour.at(":03").do(
        synch_aws_iot_shadow_with_aws_rds_postgres_config.main, c=c
    )
    schedule.every(10).minutes.do(update_info_from_shadows.main, c=c, commit=True)

    return None


def run_schedule() -> None:
    """
    Run the schedule of tasks and wait 5 seconds before running the scheduled tasks again.
    This loop will run forever.
    """

    # Configure the logger
    c = Config()
    c.logger = configure_logging(__name__, logfile_name="main_scheduler")

    # Make the schedule
    make_schedule(c=c)

    c.logger.info("App running ✅. Running scheduled tasks forever...")
    while True:
        # Run all scheduled tasks
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    run_schedule()
