"""
Refresh the time series materialized views in TimescaleDB.
This should be run at least every 15 minutes or so, to ensure the
continuously-aggregated materialized views are up to date.
"""

import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.sql import SQL, Identifier

from project.logger_config import logger
from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
    send_error_messages,
    utcnow_naive,
)

LOGFILE_NAME = "time_series_mv_refresh"

# Number of power units to process per batch when by_power_unit=True.
# 50 units keeps DataFrame memory under ~40 MB while reducing DB round-trips
# compared to single-unit processing (287 queries -> ~6 queries).
BATCH_SIZE = 50


def _build_power_unit_filter(power_units: list[str] | None) -> str:
    """Build a SQL WHERE clause fragment for filtering by power unit(s).

    Returns an empty string if power_units is None (no filter).
    """
    if not power_units:
        return ""
    placeholders = ", ".join(f"'{pu}'" for pu in power_units)
    return f" and power_unit IN ({placeholders})"


def get_min_latest_timestamp_for_batch(power_units: list[str]) -> datetime:
    """Get the oldest 'latest timestamp' across a batch of power units.

    Returns the minimum of the per-unit max timestamps, ensuring we fetch
    enough data to bring ALL units in the batch up to date.
    """
    pu_filter = _build_power_unit_filter(power_units)
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
            SELECT MIN(latest_ts) as timestamp_utc FROM (
                SELECT power_unit, MAX(timestamp_utc) as latest_ts
                FROM public.time_series_locf
                WHERE timestamp_utc >= (now() - interval '{interval}')
                {pu_filter}
                GROUP BY power_unit
            ) sub
        """
        _, rows = run_query(sql, db="timescale", fetchall=True, raise_error=True)
        timestamp = rows[0]["timestamp_utc"]
        if timestamp:
            return timestamp

    raise ValueError(
        f"Couldn't find timestamps for any of {len(power_units)} power units "
        f"(first 5: {power_units[:5]})"
    )


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
#     run_query(sql_refresh_locf_mv, db="timescale", commit=True, raise_error=True)

#     return True


def get_latest_timestamp_in_table(
    table: str = "time_series_locf",
    raise_error: bool = True,
    threshold: timedelta = None,
    power_unit_str: str = None,
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

        _, rows = run_query(sql, db="timescale", fetchall=True, raise_error=raise_error)

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
                table=table, raise_error=True, threshold=time_delta
            )
        except Exception as err:
            filename = Path(__file__).name
            send_error_messages(
                c=c, err=err, filename=filename, want_email=True, want_sms=True
            )

    return True


def get_gateway_power_unit_dict() -> dict:
    """Get the gateway and power unit mapping"""
    SQL_GW_PU = """
        select
            t2.aws_thing as gateway,
            t1.power_unit_str
        from public.power_units t1
        left join public.gw t2
            on t2.power_unit_id = t1.id
        where t1.power_unit_str is not null
            and t2.gateway is not null
    """
    columns, rows = run_query(SQL_GW_PU, db="ijack", fetchall=True, raise_error=True)
    df = pd.DataFrame(rows, columns=columns)
    dict_ = dict(zip(df["gateway"], df["power_unit_str"]))
    return dict_


def get_power_units_in_service() -> list:
    """Get the gateway and power unit mapping, for power units that are in service"""
    SQL_POWER_UNITS = """
        select 
            distinct t1.power_unit_str
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
    _, rows = run_query(SQL_POWER_UNITS, db="ijack", fetchall=True, raise_error=True)
    return [row["power_unit_str"] for row in rows]


def get_and_insert_latest_values(
    after_this_date: datetime,
    power_units: list[str] | None,
    gateway_power_unit_dict: dict,
) -> bool:
    """
    Get the latest values from the "last one carried forward" materialized view,
    which are not already in the regular "copied" table, and insert them.
    This should allow us to run continuous aggregates on the "regular copied table".

    Args:
        after_this_date: Only fetch data after this timestamp.
        power_units: List of power unit strings to process, or None for all units.
        gateway_power_unit_dict: Mapping of gateway to power unit.
    """
    dt_x_days_back_to_fill_forward: datetime = after_this_date - timedelta(days=1)
    dt_x_days_back_str: str = dt_x_days_back_to_fill_forward.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    # Only grab a ~small window of data to fill forward, at the most. Otherwise we'll overload the system.
    max_date: datetime = after_this_date + timedelta(days=90)
    max_date_str: str = max_date.strftime("%Y-%m-%d %H:%M:%S")
    after_this_date_str: str = after_this_date.strftime("%Y-%m-%d %H:%M:%S")
    pu_filter = _build_power_unit_filter(power_units)

    logger.info(
        "Getting data from LOCF table so we almost certainly have something to fill forward..."
    )
    sql_old_data = f"""
    select *
    from public.time_series_locf
    where timestamp_utc > '{dt_x_days_back_str}'
        and timestamp_utc <= '{after_this_date_str}'
        {pu_filter}
    """

    columns_old, rows_old = run_query(
        sql_old_data, db="timescale", fetchall=True, raise_error=True
    )
    time.sleep(0.25)
    df_old = pd.DataFrame(rows_old, columns=columns_old)
    del rows_old

    logger.info(
        "Getting new data from the regular time_series table (which contains nulls), to be efficiently inserted into the LOCF table we previously queried above..."
    )
    sql_latest_data = f"""
    select *
    from public.time_series
    where timestamp_utc > '{after_this_date_str}'
        and timestamp_utc <= '{max_date_str}'
        {pu_filter}
    """

    columns_new, rows_new = run_query(
        sql_latest_data, db="timescale", fetchall=True, raise_error=True
    )
    time.sleep(0.25)
    df_new = pd.DataFrame(rows_new, columns=columns_new)
    time.sleep(0.1)
    del rows_new

    if len(df_new) == 0:
        logger.warning(
            "No new data found in 'time_series' for %s power unit(s) after '%s' and before '%s'",
            len(power_units) if power_units else "all",
            after_this_date_str,
            max_date_str,
        )
        return False

    # Join the old and new dataframes together
    df = pd.concat([df_old, df_new], ignore_index=True)
    del df_old
    del df_new

    # Filter out test data since it sometimes violates database unique constraint
    df = df[~((df["power_unit"] == "111111") | (df["gateway"] == "lambda_access"))]

    # If there's a gateway and no power unit, fill in the power unit
    # Show the records that are missing a power unit
    df_missing_power_unit = df[df["power_unit"].isnull()]
    n_records_missing_power_unit = len(df_missing_power_unit)
    if n_records_missing_power_unit > 0:
        logger.info(
            "Number of records missing a power unit: %s", n_records_missing_power_unit
        )
        logger.info("Ensuring the power unit is filled in...")
        df["power_unit"] = np.where(
            df["gateway"] & ~df["power_unit"],
            df["gateway"].map(gateway_power_unit_dict),
        )

    # For the power unit, convert to string and remove the ".0" if it's there
    logger.info(
        "Converting the power unit to a string and removing the '.0' if it's there..."
    )
    # time_start = time.time()
    df["power_unit"] = df["power_unit"].astype(str).str.replace(r"\.0$", "", regex=True)
    # mins_taken = round((time.time() - time_start) / 60, 1)
    # logger.info(
    #     "Minutes taken to ensure the power unit and gateway are filled in: %s",
    #     mins_taken,
    # )

    # If values are missing, it's because they were the same as the previous values so they weren't sent
    # OPTIMIZED: Column-by-column groupby ffill/bfill — O(n) time, low memory
    # Processes one column at a time to avoid creating a full-width intermediate DataFrame
    # that would exceed the 512 MB container memory limit with production data volumes.
    # See test/test_time_series_mv_refresh_optimization.py for validation tests
    logger.info("Sorting and filling in missing values (column-by-column)...")
    time_start = time.time()

    # Step 1: Sort by power_unit and timestamp (enables proper ffill/bfill within groups)
    df = df.sort_values(["power_unit", "timestamp_utc"])

    # Step 2: Identify columns to fill (all except identifiers and timestamps)
    exclude_cols = ["timestamp_utc", "timestamp_utc_inserted", "power_unit", "gateway"]
    fill_columns = [col for col in df.columns if col not in exclude_cols]

    # Step 3: Column-by-column ffill/bfill to control memory usage
    # Each iteration creates a single-column intermediate (~1.9 MB) instead of
    # all ~127 columns at once (~238 MB), keeping peak memory under the container limit.
    n_power_units = df["power_unit"].nunique()
    logger.info(
        f"Forward/backward filling {len(fill_columns)} columns across {n_power_units} power units..."
    )
    grouped = df.groupby("power_unit", sort=False)
    for col in fill_columns:
        df[col] = grouped[col].ffill()
        df[col] = grouped[col].bfill()

    mins_taken = round((time.time() - time_start) / 60, 1)
    logger.info(
        "Minutes taken to sort and fill in missing values: %s",
        mins_taken,
    )

    # Change data types to match the database table
    # Change signal to smallint in postgres. Must be 'Int64' in pandas to allow for NaNs
    df["signal"] = df["signal"].astype("Int64")

    logger.info("Initializing a string buffer of the CSV data...")
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
    logger.info(f"{minutes_taken} minutes to create the string buffer of the CSV data.")

    # Copy the string buffer to the AWS PostgreSQL table
    query = SQL(
        """
        COPY {table} ({columns})
        FROM STDIN
        WITH (FORMAT 'csv', HEADER false, DELIMITER ',', NULL '', ENCODING 'UTF-8')
    """
    ).format(
        table=Identifier("public", "time_series_locf"),
        columns=SQL(", ").join([Identifier(c) for c in df.columns]),
    )

    time_start = time.time()
    try:
        run_query(
            sql=None,
            db="timescale",
            fetchall=False,
            commit=True,
            raise_error=True,
            copy_expert_kwargs={"sql": query, "file": sio, "size": 8192},
        )
    except psycopg2.errors.UniqueViolation as err:
        logger.error("UniqueViolation during COPY (duplicate rows skipped): %s", err)
    except Exception:
        logger.exception("ERROR running cursor.copy_from(sio...)!")
        raise

    minutes_taken = round((time.time() - time_start) / 60, 1)
    logger.info(
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
    after_this_date: datetime, views_to_update: dict | None = None
) -> bool:
    """Force the continuous aggregates to refresh with the latest data"""

    views_to_update = views_to_update or {
        "time_series_mvca_20_minute_interval": timedelta(minutes=40),
        "time_series_mvca_1_hour_interval": timedelta(hours=2),
        "time_series_mvca_3_hour_interval": timedelta(hours=6),
        "time_series_mvca_6_hour_interval": timedelta(hours=12),
        "time_series_mvca_24_hour_interval": timedelta(hours=48),
    }

    for view, min_time_delta in views_to_update.items():
        try:
            logger.info(
                "Force-refreshing continuously-aggregating materialized view '%s'",
                view,
            )
            sql = get_refresh_continuous_aggregate_sql(
                view, date_begin=after_this_date, min_window=min_time_delta
            )
            # AUTOCOMMIT is set, so commit is irrelevant
            run_query(
                sql,
                db="timescale",
                commit=False,
                raise_error=True,
                # Continuous aggregates cannot be run inside transaction blocks.
                # Set AUTOCOMMIT so no transaction block is started
                isolation_level=ISOLATION_LEVEL_AUTOCOMMIT,
            )
            time.sleep(0.1)
        except Exception:
            logger.exception(
                "ERROR force-refreshing continuously-aggregating materialized view '%s'",
                view,
            )

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


@error_wrapper(filename=Path(__file__).name)
def main(c: Config, by_power_unit: bool = False) -> bool:
    """Main entrypoint function"""

    exit_if_already_running(c, Path(__file__).name)

    time_start_main = time.time()

    # Check if LOCF table is already stale at startup (indicates previous run may have failed).
    # Alert early so we know about failures from previous runs (OOM, crash, etc.).
    # Using 1 hour threshold since job runs every 30 minutes.
    check_table_timestamps(
        c, tables=["time_series_locf"], time_delta=timedelta(hours=1)
    )

    # Get the gateway: power unit mapping dictionary, which has all gateway: power unit pairs,
    # even if the power unit is no longer in service with a structure ID.
    gateway_power_unit_dict: dict = get_gateway_power_unit_dict()

    if by_power_unit:
        # Process in batches of BATCH_SIZE to balance memory usage vs DB round-trips
        all_power_units: list = get_power_units_in_service()
        batches = [
            all_power_units[i : i + BATCH_SIZE]
            for i in range(0, len(all_power_units), BATCH_SIZE)
        ]
        logger.info(
            "Processing %s power units in %s batches of up to %s",
            len(all_power_units),
            len(batches),
            BATCH_SIZE,
        )
    else:
        # None means process all power units in a single query (no filter)
        batches = [None]

    timestamp = None
    min_timestamp = None  # Track earliest timestamp for continuous aggregate refresh
    for batch_idx, batch in enumerate(batches):
        if batch is not None:
            logger.info(
                "Batch %s of %s (%s units): %s...%s",
                batch_idx + 1,
                len(batches),
                len(batch),
                batch[0],
                batch[-1],
            )

        try:
            if batch is not None:
                timestamp = get_min_latest_timestamp_for_batch(batch)
            else:
                timestamp = get_latest_timestamp_in_table(
                    table="time_series_locf", raise_error=True
                )
            if min_timestamp is None or timestamp < min_timestamp:
                min_timestamp = timestamp
        except Exception as err:
            if by_power_unit:
                logger.exception("Error getting timestamps for batch %s", batch_idx + 1)
                continue
            filename = Path(__file__).name
            send_error_messages(
                c=c, err=err, filename=filename, want_email=True, want_sms=True
            )
            raise

        get_and_insert_latest_values(
            after_this_date=timestamp,
            power_units=batch,
            gateway_power_unit_dict=gateway_power_unit_dict,
        )

    # Force the continuous aggregates to refresh, including the latest data
    if min_timestamp is not None:
        force_refresh_continuous_aggregates(after_this_date=min_timestamp)
    else:
        logger.error(
            "Skipping continuous aggregate refresh: no batches produced a valid timestamp"
        )

    # Check the table timestamps to see if they're recent.
    # Do this last so the processes above at least get a chance to correct the situation first.
    check_table_timestamps(c)

    elapsed_min = (time.time() - time_start_main) / 60
    logger.info("Total LOCF refresh completed in %.1f minutes", elapsed_min)

    return True


if __name__ == "__main__":
    c = Config()

    main(c)
