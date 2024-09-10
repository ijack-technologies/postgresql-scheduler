import sys
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

# import numpy as np

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

# local imports
from project.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    get_conn,
    get_power_units_and_unit_types,
    run_query,
    send_error_messages,
    utcnow_naive,
)

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
    power_unit_str: str = None,
    conn=None,
) -> datetime:
    """Get the most recent timestamp in public.time_series_locf"""

    timestamp = None
    for interval in [
        "2 hours",
        "12 hours",
        "2 days",
        "7 days",
        "14 days",
        "90 days",
        "180 days",
        "365 days",
    ]:
        sql = f"""
            select max(timestamp_utc) as timestamp_utc
            from public.{table}
            where timestamp_utc >= (now() - interval '{interval}')
        """
        if power_unit_str:
            sql += f" and power_unit = '{power_unit_str}'"
        # if datetime.today() < datetime(2024, 9, 11):
        #     sql += " and timestamp_utc < '2024-09-10'"

        _, rows = run_query(
            c, sql, db="timescale", conn=conn, fetchall=True, raise_error=raise_error
        )

        timestamp = rows[0]["timestamp_utc"]
        if timestamp:
            break

    if not timestamp:
        raise ValueError(
            f"Couldn't find max(timestamp_utc) from table '{table}' for power unit '{power_unit_str}'. rows = {rows} after the following query: '''{sql}'''"
        )

    if threshold:
        if not isinstance(threshold, timedelta):
            raise TypeError(
                f"threshold is not of type 'timedelta'. Instead, it's of type '{type(threshold)}'"
            )

        timestamp_threshold = utcnow_naive() - threshold
        if timestamp < timestamp_threshold:
            raise Exception(
                f"ERROR: latest timestamp in table '{table}' for power unit '{power_unit_str}' is {timestamp} which is before the threshold timedelta '{threshold}' (threshold timestamp: {timestamp_threshold}"
            )

    return timestamp


def check_table_timestamps(
    c,
    tables: list = ["time_series", "time_series_locf"],
    time_delta: timedelta = timedelta(hours=1),
    conn=None,
) -> bool:
    """Check the tables to see if their timestamps are recent"""

    if not isinstance(tables, (list, tuple)):
        raise ValueError(
            f"tables is not of type 'list'. Instead, it's of type '{type(tables)}'"
        )

    if len(tables) == 0:
        raise ValueError("There are no tables to check!")

    for table in tables:
        try:
            get_latest_timestamp_in_table(
                c, table=table, raise_error=True, threshold=time_delta, conn=conn
            )
        except Exception as err:
            filename = Path(__file__).name
            send_error_messages(
                c=c, err=err, filename=filename, want_email=True, want_sms=True
            )

    return True


# def get_gateway_power_unit_dict(c, conn=None) -> dict:
#     """Get the gateway and power unit mapping"""
#     SQL_GW_PU = """
#         select
#             t2.aws_thing as gateway,
#             t1.power_unit_str
#         from public.power_units t1
#         left join public.gw t2
#             on t2.power_unit_id = t1.id
#         where t1.power_unit_str is not null
#             and t2.gateway is not null
#     """
#     columns, rows = run_query(c, SQL_GW_PU, db="ijack", conn=conn, fetchall=True, raise_error=True)
#     df = pd.DataFrame(rows, columns=columns)
#     dict_ = dict(zip(df["gateway"], df["power_unit_str"]))
#     return dict_


def get_power_units_in_service(c) -> dict:
    """Get the gateway and power unit mapping, for power units that are in service"""
    SQL_GW_PU = """
        select 
            t2.aws_thing as gateway,
            t1.power_unit_str
        from public.power_units t1
        inner join public.gw t2
            on t2.power_unit_id = t1.id
        inner join public.structures t3
            on t3.power_unit_id = t1.id
        where t1.power_unit_str is not null
            and t2.gateway is not null
            and t3.power_unit_id is not null
            and t3.surface is not null
    """
    columns, rows = run_query(c, SQL_GW_PU, db="ijack", fetchall=True, raise_error=True)
    df = pd.DataFrame(rows, columns=columns)
    dict_ = dict(zip(df["gateway"], df["power_unit_str"]))
    return dict_


def get_and_insert_latest_values(
    c,
    after_this_date: datetime,
    power_unit_str: str,
    gateway_power_unit_dict: dict,
    conn=None,
) -> bool:
    """
    Get the latest values from the "last one carried forward" materialized view,
    which are not already in the regular "copied" table, and insert them.
    This should allow us to run continuous aggregates on the "regular copied table".
    """
    dt_x_days_back_to_fill_forward: datetime = after_this_date - timedelta(days=1)
    dt_x_days_back_str: str = dt_x_days_back_to_fill_forward.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    # Only grab a ~small window of data to fill forward, at the most. Otherwise we'll overload the system.
    max_date: datetime = after_this_date + timedelta(days=90)
    max_date_str: str = max_date.strftime("%Y-%m-%d %H:%M:%S")
    after_this_date_str: str = after_this_date.strftime("%Y-%m-%d %H:%M:%S")

    c.logger.info(
        "Getting data from LOCF table so we almost certainly have something to fill forward..."
    )
    SQL_OLD_DATA = f"""
    select *
    from public.time_series_locf
    where timestamp_utc > '{dt_x_days_back_str}'
        and timestamp_utc <= '{after_this_date_str}'
    """
    if power_unit_str:
        SQL_OLD_DATA += f" and power_unit = '{power_unit_str}'"

    columns_old, rows_old = run_query(
        c, SQL_OLD_DATA, db="timescale", conn=conn, fetchall=True, raise_error=True
    )
    c.logger.info("Sleeping for 1 second to allow the server to catch up...")
    time.sleep(1)
    df_old = pd.DataFrame(rows_old, columns=columns_old)
    del rows_old

    c.logger.info(
        "Getting new data from the regular time_series table (which contains nulls), to be efficiently inserted into the LOCF table we previously queried above..."
    )
    SQL_LATEST_DATA = f"""
    select *
    from public.time_series
    where timestamp_utc > '{after_this_date_str}'
        and timestamp_utc <= '{max_date_str}'
    """
    if power_unit_str:
        SQL_LATEST_DATA += f" and power_unit = '{power_unit_str}'"

    columns_new, rows_new = run_query(
        c, SQL_LATEST_DATA, db="timescale", conn=conn, fetchall=True, raise_error=True
    )
    c.logger.info("Sleeping for 1 second to allow the server to catch up...")
    time.sleep(1)
    df_new = pd.DataFrame(rows_new, columns=columns_new)
    time.sleep(0.5)
    del rows_new

    if len(df_new) == 0:
        # raise ValueError(
        c.logger.warning(
            f"ERROR: No new data was found in the 'time_series' table for power unit '{power_unit_str}' after the timestamp '{after_this_date_str}' and before the timestamp '{max_date_str}'"
        )
        return False

    # Join the old and new dataframes together
    df = pd.concat([df_old, df_new], ignore_index=True)
    del df_old
    del df_new

    # Filter out test data since it sometimes violates database unique constraint
    df = df[~((df["power_unit"] == "111111") | (df["gateway"] == "lambda_access"))]
    if power_unit_str == "200480":
        print(f"We found power unit {power_unit_str}!")

    # If there's a gateway and no power unit, fill in the power unit
    # Show the records that are missing a power unit
    df_missing_power_unit = df[df["power_unit"].isnull()]
    n_records_missing_power_unit = len(df_missing_power_unit)
    if n_records_missing_power_unit > 0:
        c.logger.info(
            "Number of records missing a power unit: %s", n_records_missing_power_unit
        )
        c.logger.info("Ensuring the power unit is filled in...")
        df["power_unit"] = np.where(
            df["gateway"] & ~df["power_unit"],
            df["gateway"].map(gateway_power_unit_dict),
        )

    # For the power unit, convert to string and remove the ".0" if it's there
    c.logger.info(
        "Converting the power unit to a string and removing the '.0' if it's there..."
    )
    # time_start = time.time()
    df["power_unit"] = df["power_unit"].astype(str).str.replace(r"\.0$", "", regex=True)
    # mins_taken = round((time.time() - time_start) / 60, 1)
    # c.logger.info(
    #     "Minutes taken to ensure the power unit and gateway are filled in: %s",
    #     mins_taken,
    # )

    # If values are missing, it's because they were the same as the previous values so they weren't sent
    c.logger.info("Sorting and filling in missing values...")
    n_power_units = len(df["power_unit"].unique())
    power_unit_counter = 0
    time_start = time.time()
    # Pandas 3.0 future defaults: copy_on_write = True and no_silent_downcasting = True
    pd.options.mode.copy_on_write = True
    pd.set_option("future.no_silent_downcasting", True)
    for power_unit, group in df.groupby("power_unit"):
        power_unit_counter += 1
        c.logger.info(
            "Sorting and filling %s of %s... Group size for power_unit %s: %s",
            power_unit_counter,
            n_power_units,
            power_unit,
            len(group),
        )
        # Sort by timestamp_utc, then fill in missing values
        sorted_group = (
            group.sort_values("timestamp_utc", ascending=True)
            .infer_objects()
            .ffill()
            .bfill()
        )
        # Replace the original group with the sorted and filled group
        df.loc[df["power_unit"] == power_unit, :] = sorted_group
        time.sleep(0.1)

    mins_taken = round((time.time() - time_start) / 60, 1)
    c.logger.info(
        "Minutes taken to sort and fill in missing values: %s",
        mins_taken,
    )

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
    try:
        run_query(
            c,
            sql=None,
            db="timescale",
            commit=True,
            conn=conn,
            # Need to see the errors if they occur!
            raise_error=True,
            copy_from_kwargs={
                "file": sio,
                # For some reason it doesn't work if you put the schema in the table name
                "table": "time_series_locf",
                "sep": ",",
                "null": "",
                "size": 8192,
                "columns": df.columns,
            },
        )
    except Exception as err:
        if "UniqueViolation" in str(err):
            c.logger.error(err)
        else:
            c.logger.exception("ERROR running cursor.copy_from(sio...)!")
            raise

    minutes_taken = round((time.time() - time_start) / 60, 1)
    c.logger.info(
        f"{minutes_taken} minutes to run efficient copy_from(file=sio) operation!"
    )

    return True


def get_refresh_continuous_aggregate_sql(
    name: str,
    date_begin: datetime,
    date_end: datetime = utcnow_naive(),
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
    c, after_this_date: datetime, views_to_update: dict = None, conn=None
):
    """Force the continuous aggregates to refresh with the latest data"""

    views_to_update = views_to_update or {
        "time_series_mvca_20_minute_interval": timedelta(minutes=40),
        "time_series_mvca_1_hour_interval": timedelta(hours=2),
        "time_series_mvca_3_hour_interval": timedelta(hours=6),
        "time_series_mvca_6_hour_interval": timedelta(hours=12),
        "time_series_mvca_24_hour_interval": timedelta(hours=48),
    }

    if conn:
        should_close_connection: bool = False
        isolation_level_before = conn.isolation_level
    else:
        conn = get_conn(c, db="timescale")
        should_close_connection = True
        isolation_level_before = None

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
            time.sleep(0.5)
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

    if should_close_connection:
        conn.close()
    else:
        conn.set_isolation_level(isolation_level_before)

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
def main(c: Config, by_power_unit: bool = True) -> bool:
    """Main entrypoint function"""

    exit_if_already_running(c, Path(__file__).name)

    # # NOTE the following is only for ad hoc purposes;
    # # it only runs if the date is before a certain date!
    # # It also runs again after the latest values are inserted, below!
    # ad_hoc_maybe_refresh_continuous_aggs()

    # # First refresh the main "last one carried forward" materialized view
    # refresh_locf_materialized_view(c)

    conn_ts = get_conn(c, db="timescale")

    try:
        if by_power_unit:
            # if datetime.today() < datetime(2024, 9, 11):
            #     power_unit_uno_egas_dict = {"200480": "egas"}
            # else:
            power_unit_uno_egas_dict: dict = get_power_units_and_unit_types(c)
        else:
            power_unit_uno_egas_dict = {None: None}

        # Get the gateway and power unit mapping
        # gateway_power_unit_dict = get_gateway_power_unit_dict(c)
        gateway_power_unit_dict = get_power_units_in_service(c)

        n_power_units = len(power_unit_uno_egas_dict)
        for index, (power_unit_str, is_egas_type) in enumerate(
            power_unit_uno_egas_dict.items()
        ):
            c.logger.info(
                "Power unit %s of %s: %s - %s",
                index + 1,
                n_power_units,
                power_unit_str,
                "EGAS type" if is_egas_type else "UNO type",
            )

            # Get the lastest values from the LOCF MV and insert
            # into the regular table, to trigger the continuous aggregates to refresh
            try:
                # Do this first so we get an error email if the latest timestamp is too old,
                # Even if the process below corrects the situation. This ensures
                # we know about the error even if the process below FAILS!
                timestamp = get_latest_timestamp_in_table(
                    c,
                    table="time_series_locf",
                    raise_error=True,
                    power_unit_str=power_unit_str,
                    conn=conn_ts,
                )
            except Exception as err:
                filename = Path(__file__).name
                send_error_messages(
                    c=c, err=err, filename=filename, want_email=True, want_sms=True
                )
                raise

            try:
                get_and_insert_latest_values(
                    c,
                    after_this_date=timestamp,
                    power_unit_str=power_unit_str,
                    gateway_power_unit_dict=gateway_power_unit_dict,
                    conn=conn_ts,
                )
            except psycopg2.errors.UniqueViolation as err:
                c.logger.error(err)
                conn_ts.rollback()

        # Force the continuous aggregates to refresh, including the latest data
        force_refresh_continuous_aggregates(c, after_this_date=timestamp, conn=conn_ts)

        # Check the table timestamps to see if they're recent.
        # Do this last so the processes above at least get a chance to correct the situation first.
        check_table_timestamps(c, conn=conn_ts)
    finally:
        conn_ts.close()

    return True


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(__name__, logfile_name=LOGFILE_NAME)
    main(c)
