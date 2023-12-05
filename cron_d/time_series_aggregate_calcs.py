"""
This script recalculates some aggregated data on a daily basis, for performance calculations.
"""

import time
import os
from datetime import datetime, timedelta
import logging
import pathlib
import sys
from io import StringIO

import psycopg2
import pandas as pd

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
LOGFILE_NAME = "time_series_aggregate_calcs"


def get_power_units_and_unit_types(c) -> dict:
    """Get the power units and unit types mapping"""
    sql = """
    SELECT
        id as structure_id,
        power_unit_id,
        power_unit_str,
        power_unit_type,
        gateway_id,
        aws_thing,
        unit_type_id,
        unit_type,
        case when unit_type_id in (1, 4) then false else true end as is_egas_type,
        model_type_id,
        model,
        model_unit_type_id,
        model_unit_type,
        customer_id,
        customer
    FROM public.vw_structures_joined
        --structure must have a power unit
        where power_unit_id is not null
            --power unit must have a gateway or there's no data
            and gateway_id is not null
    """
    columns, rows = run_query(
        c, sql, db="ijack", fetchall=True, raise_error=True, log_query=False
    )
    df = pd.DataFrame(rows, columns=columns)
    dict_ = dict(zip(df["power_unit_str"], df["is_egas_type"]))
    return dict_


def get_time_series_data(c, power_unit_str: str, is_egas_type: bool) -> pd.DataFrame:
    """
    Get the time series data for a given power unit
    """
    # spm_col = "stroke_speed_avg"
    # if is_egas_type:
    #     hp_col = "hpe"
    #     ht_col = "ht_egas"
    # else:
    #     hp_col = "hpu"
    #     ht_col = "ht"
    # Get data from LOCF table so it's filled forward
    sql = """
    select
        power_unit,
        date_trunc('month', timestamp_utc) as month_date,
        count(*) as sample_size,
        avg(stroke_speed_avg) as stroke_speed_avg,
        avg(hp_limit) as hp_limit,
        avg(GREATEST(hp_raising_avg, hp_lowering_avg)) as hp_avg,
        --derate discharge setpoint (mgp)
        avg(mgp) as mgp_avg,
        avg(dgp) as dgp_avg,
        --Max discharge temperature before derates (C)
        avg(agf_dis_temp_max) as agf_dis_temp_max_avg,
        avg(agf_dis_temp) as agf_dis_temp_avg
    from public.time_series_locf
    where power_unit = '{}' {}
    group by power_unit, date_trunc('month', timestamp_utc)
    """
    if c.DEV_TEST_PRD == "development":
        # Recalculate all months
        sql = sql.format(
            power_unit_str,
            "",
        )
    else:
        # Only recalculate the current month
        sql = sql.format(
            power_unit_str,
            "and timestamp_utc >= date_trunc('month', now())",
        )
    columns, rows = run_query(
        c, sql, db="timescale", fetchall=True, raise_error=True, log_query=False
    )
    df = pd.DataFrame(rows, columns=columns)
    df["month_date"] = pd.to_datetime(df["month_date"])

    return df


def upsert_time_series_agg(c, power_unit_str: str, df: pd.DataFrame) -> bool:
    """
    Upsert the time series aggregate data for a given power unit
    """
    # Group by month_date with Pandas DataFrame
    grouped_df = df.groupby("month_date")
    num_months = grouped_df.ngroups
    for index, (month_date, df_month_date) in enumerate(grouped_df):
        month_date_str = month_date.strftime("%Y-%m-%d")
        c.logger.info(
            "Month %s of %s: %s - %s",
            index + 1,
            num_months,
            power_unit_str,
            month_date_str,
        )
        # Do an upsert to update the data in the database
        timestamp_utc_modified = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # sql_delete = (
        #     f"""
        # DELETE FROM public.time_series_agg
        # WHERE power_unit = '{power_unit_str}'
        #     AND month_date = '{month_date_str}'
        # """.replace(
        #         "\n", " "
        #     )
        #     .replace("nan", "null")
        #     .replace("None", "null")
        # )
        # run_query(
        #     c,
        #     sql_delete,
        #     db="timescale",
        #     commit=True,
        #     raise_error=True,
        #     log_query=False,
        # )
        # Upsert seems to be faster than delete and insert
        sql_upsert = (
            f"""
        INSERT INTO public.time_series_agg(
            power_unit, month_date, timestamp_utc_modified, sample_size, stroke_speed_avg, hp_limit, hp_avg, mgp_avg, dgp_avg, agf_dis_temp_max_avg, agf_dis_temp_avg
        )
        VALUES (
            '{power_unit_str}',
            '{month_date}',
            '{timestamp_utc_modified}',
            {df_month_date['sample_size'].iloc[0]},
            {df_month_date['stroke_speed_avg'].iloc[0]},
            {df_month_date['hp_limit'].iloc[0]},
            {df_month_date['hp_avg'].iloc[0]},
            {df_month_date['mgp_avg'].iloc[0]},
            {df_month_date['dgp_avg'].iloc[0]},
            {df_month_date['agf_dis_temp_max_avg'].iloc[0]},
            {df_month_date['agf_dis_temp_avg'].iloc[0]}
        )
        ON CONFLICT (power_unit, month_date) DO UPDATE
        SET
            timestamp_utc_modified = '{timestamp_utc_modified}',
            sample_size = {df_month_date['sample_size'].iloc[0]},
            stroke_speed_avg = {df_month_date['stroke_speed_avg'].iloc[0]},
            hp_limit = {df_month_date['hp_limit'].iloc[0]},
            hp_avg = {df_month_date['hp_avg'].iloc[0]},
            mgp_avg = {df_month_date['mgp_avg'].iloc[0]},
            dgp_avg = {df_month_date['dgp_avg'].iloc[0]},
            agf_dis_temp_max_avg = {df_month_date['agf_dis_temp_max_avg'].iloc[0]},
            agf_dis_temp_avg = {df_month_date['agf_dis_temp_avg'].iloc[0]}
        """.replace(
                "\n", " "
            )
            .replace("nan", "null")
            .replace("None", "null")
        )
        run_query(
            c,
            sql_upsert,
            db="timescale",
            commit=True,
            raise_error=True,
            log_query=False,
        )

    return True


@error_wrapper()
def main(c) -> bool:
    """Main entrypoint function"""

    exit_if_already_running(c, pathlib.Path(__file__).name)

    power_unit_uno_egas_dict: dict = get_power_units_and_unit_types(c)

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

        df: pd.DataFrame = get_time_series_data(
            c, power_unit_str=power_unit_str, is_egas_type=is_egas_type
        )
        if df.empty:
            c.logger.warning(
                "No data found for power unit '%s'",
                power_unit_str,
            )
            continue

        upsert_time_series_agg(c=c, power_unit_str=power_unit_str, df=df)

        # Give other apps a chance to run
        time.sleep(0.5)

    c.logger.info("All done!")
    return True


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
