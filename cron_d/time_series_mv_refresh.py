import logging
import os
import pathlib
import sys
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import psycopg2

# import numpy as np

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = str(pathlib.Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

# local imports
from cron_d.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    get_conn,
    run_query,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "time_series_mv_refresh"


# def refresh_locf_materialized_view(c):
#     """
#     Refresh the "last one carried forward" or "filled forward"
#     time series view. This is extremely important for the
#     aggregated queries that come later, so the zeros are
#     properly carried forward.
#     """

#     # Requires owner privileges (must be run by "master" user, not "app_user")
#     sql_refresh_locf_mv = """
#         REFRESH MATERIALIZED VIEW CONCURRENTLY
#         public.time_series_locf
#         WITH DATA;
#     """
#     run_query(c, sql_refresh_locf_mv, db="timescale", commit=True, raise_error=True)

#     return True


def get_latest_timestamp_in_table(
    c,
    table: str = "time_series_locf",
    raise_error: bool = True,
    threshold: timedelta = None,
) -> datetime:
    """Get the most recent timestamp in public.time_series_locf"""

    for interval in ["7 days", "14 days", "90 days", "180 days", "365 days"]:
        sql = f"""
            select max(timestamp_utc) as timestamp_utc
            from public.{table}
            where timestamp_utc >= (now() - interval '{interval}')
        """
        _, rows = run_query(c, sql, db="timescale", fetchall=True, raise_error=raise_error)

        timestamp = rows[0]["timestamp_utc"]
        if timestamp:
            break

    if not timestamp:
        raise ValueError(
            f"Couldn't find max(timestamp_utc) from table '{table}'. rows = {rows} after the following query: '''{sql}'''"
        )

    if threshold:
        if not isinstance(threshold, timedelta):
            raise TypeError(
                f"threshold is not of type 'timedelta'. Instead, it's of type '{type(threshold)}'"
            )

        timestamp_threshold = datetime.utcnow() - threshold
        if timestamp < timestamp_threshold:
            raise Exception(
                f"ERROR: latest timestamp in table '{table}' is {timestamp} which is before the threshold timedelta '{threshold}' (threshold timestamp: {timestamp_threshold}"
            )

    return timestamp


def check_table_timestamps(
    c,
    tables: list = ["time_series", "time_series_locf"],
    time_delta: timedelta = timedelta(hours=1),
) -> bool:
    """Check the tables to see if their timestamps are recent"""

    if not isinstance(tables, (list, tuple)):
        raise ValueError(
            f"tables is not of type 'list'. Instead, it's of type '{type(tables)}'"
        )

    if len(tables) == 0:
        raise ValueError("There are no tables to check!")

    for table in tables:
        get_latest_timestamp_in_table(
            c, table=table, raise_error=True, threshold=time_delta
        )

    return True


def get_gateway_power_unit_dict(c) -> dict:
    """Get the gateway and power unit mapping"""
    SQL_GW_PU = """
        select gateway, power_unit_str
        from public.gateways
        where power_unit_str is not null
            and gateway is not null
    """
    columns, rows = run_query(c, SQL_GW_PU, db="ijack", fetchall=True, raise_error=True)
    df = pd.DataFrame(rows, columns=columns)
    dict_ = dict(zip(df["gateway"], df["power_unit_str"]))
    return dict_


# OPTIONS_DICT = {
#     "connect_timeout": 5,
#     "cursor_factory": DictCursor,
#     # whether client-side TCP keepalives are used
#     "keepalives": 1,
#     # seconds of inactivity after which TCP should send a keepalive message to the server
#     "keepalives_idle": 20,
#     # seconds after which a TCP keepalive message that is not acknowledged by the server should be retransmitted
#     "keepalives_interval": 10,
#     # TCP keepalives that can be lost before the client's connection to the server is considered dead
#     "keepalives_count": 5,
#     # milliseconds that transmitted data may remain unacknowledged before a connection is forcibly closed
#     "tcp_user_timeout": 0,
# }


def get_and_insert_latest_values(c, after_this_date: datetime):
    """
    Get the latest values from the "last one carried forward" materialized view,
    which are not already in the regular "copied" table, and insert them.
    This should allow us to run continuous aggregates on the "regular copied table".
    """
    dt_x_days_back_to_fill_forward = after_this_date - timedelta(days=1)
    dt_x_days_back_str = dt_x_days_back_to_fill_forward.strftime("%Y-%m-%d %H:%M:%S")
    after_this_date_str = after_this_date.strftime("%Y-%m-%d %H:%M:%S")

    # First get data from LOCF table so we almost certainly have something to fill forward
    SQL_OLD_DATA = f"""
    select *
    from public.time_series_locf
    where timestamp_utc > '{dt_x_days_back_str}'
        and timestamp_utc <= '{after_this_date_str}'
    """
    columns_old, rows_old = run_query(
        c, SQL_OLD_DATA, db="timescale", fetchall=True, raise_error=True
    )
    df_old = pd.DataFrame(rows_old, columns=columns_old)
    del rows_old

    # Now get new data from the regular time_series table (which contains nulls),
    # to be efficiently inserted into the LOCF table we previously queried above.
    SQL_LATEST_DATA = f"""
    select *
    from public.time_series
    where timestamp_utc > '{after_this_date_str}'
    """
    columns_new, rows_new = run_query(
        c, SQL_LATEST_DATA, db="timescale", fetchall=True, raise_error=True
    )
    df_new = pd.DataFrame(rows_new, columns=columns_new)
    del rows_new

    # Join the old and new dataframes together
    df = pd.concat([df_old, df_new], ignore_index=True)
    del df_old
    del df_new

    # Filter out test data since it sometimes violates database unique constraint
    df = df[~((df["power_unit"] == "111111") | (df["gateway"] == "lambda_access"))]

    # Get the gateway and power unit mapping
    gateway_power_unit_dict = get_gateway_power_unit_dict(c)
    power_unit_gateway_dict = {pu: gw for gw, pu in gateway_power_unit_dict.items()}

    c.logger.info(
        "Ensuring the power unit and gateway are filled in (this takes way too long)..."
    )
    # # Fill in power unit if there's not power unit, but there is a gateway
    # def get_power_unit_by_gateway(gateway: str) -> str:
    #     power_unit = gateway_power_unit_dict.get(gateway, None)
    #     return power_unit

    def ensure_power_unit_and_gateway(series: pd.Series) -> pd.Series:
        """Ensure there's both a power unit and a gateway"""

        series.power_unit = str(series.power_unit).replace(".0", "")

        if series.power_unit and not series.gateway:
            series.gateway = power_unit_gateway_dict.get(series.power_unit, None)
        elif series.gateway and not series.power_unit:
            series.power_unit = gateway_power_unit_dict.get(series.gateway, None)

        return series

    time_start = time.time()
    df = df.apply(ensure_power_unit_and_gateway, axis=1)
    mins_taken = round((time.time() - time_start) / 60, 1)
    c.logger.info(
        "Minutes taken to ensure the power unit and gateway are filled in: %s",
        mins_taken,
    )

    # # Fill in power_unit if there's no power_unit, but there is a gateway
    # df["gateway"] = df["power_unit"].map(gateway_power_unit_dict)
    # # df["power_unit"] = np.where(
    # #     (df["power_unit"].isna()) & (~df["gateway"].isna()),
    # #     gateway_power_unit_dict.get(df["gateway"], None),
    # #     df["power_unit"],
    # # )
    # # df.loc[df["power_unit"].isna(), "power_unit"] =

    # # Fill in gateway if there's no gateway, but there is a power unit
    # df["power_unit"] = df["gateway"].map(gateway_power_unit_dict)
    # # df["gateway"] = np.where(
    # #     (df["gateway"].isna()) & (~df["power_unit"].isna()),
    # #     power_unit_gateway_dict.get(df["power_unit"], None),
    # #     df["gateway"],
    # # )

    # If values are missing, it's because they were the same as the previous values so they weren't sent
    c.logger.info("Sorting and filling in missing values...")
    # df = df.sort_values(["gateway", "timestamp_utc"], ascending=True).groupby("gateway").ffill().bfill()
    for power_unit, group in df.groupby("power_unit"):
        c.logger.info("Group size for power_unit %s: %s", power_unit, len(group))
        # Sort by timestamp_utc, then fill in missing values
        sorted_group = (
            group.sort_values("timestamp_utc", ascending=True).ffill().bfill()
        )
        # Replace the original group with the sorted and filled group
        df.loc[df["power_unit"] == power_unit, :] = sorted_group

    # Change data types to match the database table
    # Change signal to smallint in postgres. Must be 'Int64' in pandas to allow for NaNs
    df["signal"] = df["signal"].astype("Int64")

    c.logger.info("Initializing a string buffer of the CSV data...")
    time_start = time.time()
    sio = StringIO()
    # Write the Pandas DataFrame as a CSV to the buffer
    sio.write(
        df.loc[df["timestamp_utc"] > after_this_date].to_csv(
            index=None, header=None, sep=",", encoding="utf-8"
        )
    )
    # Be sure to reset the position to the start of the stream
    sio.seek(0)
    minutes_taken = round((time.time() - time_start) / 60, 1)
    c.logger.info(
        f"{minutes_taken} minutes to create the string buffer of the CSV data."
    )

    time_start = time.time()
    with psycopg2.connect(
        host=os.getenv("HOST_TS"),
        port=os.getenv("PORT_TS"),
        dbname="ijack",
        user=os.getenv("USER_TS"),
        password=os.getenv("PASS_TS"),
        # **OPTIONS_DICT
    ) as conn:
        with conn.cursor() as cursor:
            # For some reason it doesn't work if you put the schema in the table name
            # table = "public.time_series_locf"
            table = "time_series_locf"

            # NOTE: Don't put this into a try/except because then we won't see
            # errors that prevent ALL data from being inserted!
            # try:
            cursor.copy_from(
                file=sio, table=table, sep=",", null="", size=8192, columns=df.columns
            )
            conn.commit()
            # except Exception as err:
            #     if "UniqueViolation" in str(err):
            #         c.logger.error(err)
            #     else:
            #         c.logger.exception("ERROR running cursor.copy_from(sio...)!")
            #         raise

    minutes_taken = round((time.time() - time_start) / 60, 1)
    c.logger.info(
        f"{minutes_taken} minutes to run efficient copy_from(file=sio) operation!"
    )

    return True


def get_refresh_continuous_aggregate_sql(
    name: str,
    date_begin: datetime,
    date_end: datetime = datetime.utcnow(),
    min_window: timedelta = timedelta(minutes=21),
):
    """
    The refresh command takes three arguments:
        The name of the continuous aggregate view to refresh
        The timestamp of the beginning of the refresh window
        The timestamp of the end of the refresh window
    """
    global c
    refresh_window_timespan = date_end - date_begin
    if refresh_window_timespan < min_window:
        date_begin = date_end - min_window

    dt_format = "%Y-%m-%d %H:%M:%S"
    dt_beg_str = date_begin.strftime(dt_format)
    dt_end_str = date_end.strftime(dt_format)

    return (
        f"CALL refresh_continuous_aggregate('{name}', '{dt_beg_str}', '{dt_end_str}');"
    )


def force_refresh_continuous_aggregates(
    c, after_this_date: datetime, views_to_update: dict = None
):
    """Force the continuous aggregates to refresh with the latest data"""

    views_to_update = views_to_update or {
        "time_series_mvca_20_minute_interval": timedelta(minutes=40),
        "time_series_mvca_1_hour_interval": timedelta(hours=2),
        "time_series_mvca_3_hour_interval": timedelta(hours=6),
        "time_series_mvca_6_hour_interval": timedelta(hours=12),
        "time_series_mvca_24_hour_interval": timedelta(hours=48),
    }
    conn = get_conn(c, db="timescale")
    # Continuous aggregates cannot be run inside transaction blocks.
    # Set AUTOCOMMIT so no transaction block is started
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    for view, min_time_delta in views_to_update.items():
        try:
            c.logger.info(
                "Force-refreshing continuously-aggregating materialized view '%s'", view
            )
            sql = get_refresh_continuous_aggregate_sql(
                view, date_begin=after_this_date, min_window=min_time_delta
            )
            # AUTOCOMMIT is set, so commit is irrelevant
            run_query(c, sql, db="timescale", commit=False, conn=conn, raise_error=True)
        except psycopg2.errors.InvalidParameterValue:
            c.logger.exception(
                "ERROR force-refreshing continuously-aggregating materialized view '%s'",
                view,
            )
        except Exception:
            c.logger.exception(
                "ERROR force-refreshing continuously-aggregating materialized view '%s'",
                view,
            )

    # cursor.close()
    conn.close()

    return True


def ad_hoc_maybe_refresh_continuous_aggs() -> None:
    """
    NOTE the following is only for ad hoc purposes;
    it only runs if the date is before a certain date!
    """
    dt_today = datetime.today()
    if dt_today < datetime(2022, 7, 20, 17):
        # This is just for ad hoc force-refreshing the continuously-aggregated materialized views,
        # starting at a certain date.
        # Run this with the "real_time_series_mv_refresh.py" module, if you like.
        refresh_all_after_this_date = datetime(2020, 1, 1)
        force_refresh_continuous_aggregates(
            c,
            after_this_date=refresh_all_after_this_date,
            views_to_update={
                "time_series_mvca_1_hour_interval": timedelta(hours=2),
                "time_series_mvca_20_minute_interval": timedelta(minutes=40),
                "time_series_mvca_3_hour_interval": timedelta(hours=6),
                "time_series_mvca_6_hour_interval": timedelta(hours=12),
                "time_series_mvca_24_hour_interval": timedelta(hours=48),
            },
        )

    return None


@error_wrapper()
def main(c):
    """Main entrypoint function"""

    exit_if_already_running(c, pathlib.Path(__file__).name)

    # # NOTE the following is only for ad hoc purposes;
    # # it only runs if the date is before a certain date!
    # # It also runs again after the latest values are inserted, below!
    # ad_hoc_maybe_refresh_continuous_aggs()

    # # First refresh the main "last one carried forward" materialized view
    # refresh_locf_materialized_view(c)

    # Get the lastest values from the LOCF MV and insert
    # into the regular table, to trigger the continuous aggregates to refresh
    timestamp = get_latest_timestamp_in_table(c, table="time_series_locf")
    get_and_insert_latest_values(c, after_this_date=timestamp)

    # Force the continuous aggregates to refresh, including the latest data
    force_refresh_continuous_aggregates(c, after_this_date=timestamp)

    # Check the table timestamps to see if they're recent.
    # Do this last so the processes above at least get a chance to correct the situation first.
    check_table_timestamps(c)

    return True


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
