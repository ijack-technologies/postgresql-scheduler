"""
This script recalculates some aggregated data on a daily basis, for performance calculations.
"""

import time
from datetime import date
from pathlib import Path

import pandas as pd

from project.logger_config import logger
from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    get_power_units_and_unit_types,
    run_query,
    utcnow_naive,
)

LOGFILE_NAME = "time_series_aggregate_calcs"


def get_time_series_data(
    c, power_unit_str: str, start_date_str: str, end_date_str: str
) -> pd.DataFrame:
    """
    Get the time series data for a given power unit
    """
    # Get data from LOCF table so it's filled forward
    sql = f"""
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
        avg(agf_dis_temp) as agf_dis_temp_avg,
        avg(dtp) as dtp_avg,
        avg(dtp_max) as dtp_max_avg
    from public.time_series_locf
    where timestamp_utc >= '{start_date_str}'
        and timestamp_utc < '{end_date_str}'
        and power_unit = '{power_unit_str}'
    group by power_unit, date_trunc('month', timestamp_utc)
    """
    columns, rows = run_query(
        sql, db="timescale", fetchall=True, raise_error=True, log_query=False
    )
    df = pd.DataFrame(rows, columns=columns)
    df["month_date"] = pd.to_datetime(df["month_date"])

    return df


def get_distinct_months_for_power_unit(c, power_unit_str: str) -> list:
    """Get the distinct months for a given power unit"""
    sql = f"""
        select
            distinct date_trunc('month', timestamp_utc) as month_date
        from public.time_series_locf
        where power_unit = '{power_unit_str}'
    """
    columns, rows = run_query(
        sql, db="timescale", fetchall=True, raise_error=True, log_query=False
    )
    df = pd.DataFrame(rows, columns=columns)
    df["month_date"] = pd.to_datetime(df["month_date"])
    return df["month_date"].to_list()


def upsert_time_series_agg(
    c, power_unit_str: str, month_date_str: str, df_month_date: pd.DataFrame
) -> bool:
    """
    Upsert the time series aggregate data for a given power unit
    """
    timestamp_utc_modified_str: str = utcnow_naive().strftime("%Y-%m-%d %H:%M:%S")

    # Upsert seems to be faster than delete and insert
    sql_upsert = (
        f"""
    INSERT INTO public.time_series_agg(
        power_unit, month_date, timestamp_utc_modified, sample_size,
        stroke_speed_avg, hp_limit, hp_avg, mgp_avg, dgp_avg,
        agf_dis_temp_max_avg, agf_dis_temp_avg,
        dtp_avg, dtp_max_avg
    )
    VALUES (
        '{power_unit_str}',
        '{month_date_str}',
        '{timestamp_utc_modified_str}',
        {df_month_date['sample_size'].iloc[0]},
        {df_month_date['stroke_speed_avg'].iloc[0]},
        {df_month_date['hp_limit'].iloc[0]},
        {df_month_date['hp_avg'].iloc[0]},
        {df_month_date['mgp_avg'].iloc[0]},
        {df_month_date['dgp_avg'].iloc[0]},
        {df_month_date['agf_dis_temp_max_avg'].iloc[0]},
        {df_month_date['agf_dis_temp_avg'].iloc[0]},
        {df_month_date['dtp_avg'].iloc[0]},
        {df_month_date['dtp_max_avg'].iloc[0]}
    )
    ON CONFLICT (power_unit, month_date) DO UPDATE
    SET
        timestamp_utc_modified = '{timestamp_utc_modified_str}',
        sample_size = {df_month_date['sample_size'].iloc[0]},
        stroke_speed_avg = {df_month_date['stroke_speed_avg'].iloc[0]},
        hp_limit = {df_month_date['hp_limit'].iloc[0]},
        hp_avg = {df_month_date['hp_avg'].iloc[0]},
        mgp_avg = {df_month_date['mgp_avg'].iloc[0]},
        dgp_avg = {df_month_date['dgp_avg'].iloc[0]},
        agf_dis_temp_max_avg = {df_month_date['agf_dis_temp_max_avg'].iloc[0]},
        agf_dis_temp_avg = {df_month_date['agf_dis_temp_avg'].iloc[0]},
        dtp_avg = {df_month_date['dtp_avg'].iloc[0]},
        dtp_max_avg = {df_month_date['dtp_max_avg'].iloc[0]}
    """.replace("\n", " ")
        .replace("nan", "null")
        .replace("None", "null")
    )
    run_query(
        sql_upsert,
        db="timescale",
        commit=True,
        raise_error=True,
        log_query=False,
    )

    return True


@error_wrapper()
def main(c: Config) -> bool:
    """Main entrypoint function"""

    exit_if_already_running(c, Path(__file__).name)

    power_unit_uno_egas_dict: dict = get_power_units_and_unit_types()

    n_power_units = len(power_unit_uno_egas_dict)
    for index, (power_unit_str, is_egas_type) in enumerate(
        power_unit_uno_egas_dict.items()
    ):
        logger.info(
            "Power unit %s of %s: %s - %s",
            index + 1,
            n_power_units,
            power_unit_str,
            "EGAS type" if is_egas_type else "UNO type",
        )

        if c.DEV_TEST_PRD == "production":
            # if True:
            # If in production, only recalculate the latest month
            month_dates: list = [date.today().replace(day=1)]
        else:
            month_dates: list = get_distinct_months_for_power_unit(c, power_unit_str)

        num_months = len(month_dates)
        for index, month_date in enumerate(month_dates):
            month_date_str = month_date.strftime("%Y-%m-%d")
            next_month_date = month_date + pd.DateOffset(months=1)
            next_month_date_str = next_month_date.strftime("%Y-%m-%d")
            logger.info(
                "Month %s of %s: %s - %s",
                index + 1,
                num_months,
                power_unit_str,
                month_date_str,
            )

            df_month_date: pd.DataFrame = get_time_series_data(
                c,
                power_unit_str=power_unit_str,
                start_date_str=month_date_str,
                end_date_str=next_month_date_str,
            )
            if df_month_date.empty:
                logger.warning(
                    "No data found for power unit '%s'",
                    power_unit_str,
                )
                continue

            # Do an upsert to update the data in the database
            upsert_time_series_agg(
                c=c,
                power_unit_str=power_unit_str,
                month_date_str=month_date_str,
                df_month_date=df_month_date,
            )

        # Give other apps a chance to run after each power unit
        time.sleep(0.5)

    logger.info("All done!")
    return True


if __name__ == "__main__":
    c = Config()

    main(c)
