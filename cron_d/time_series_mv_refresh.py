from datetime import datetime, timedelta
import logging
import pathlib
import sys
import psycopg2

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


def refresh_locf_materialized_view(c):
    """
    Refresh the "last one carried forward" or "filled forward"
    time series view. This is extremely important for the
    aggregated queries that come later, so the zeros are
    properly carried forward.
    """

    # Requires owner privileges (must be run by "master" user, not "app_user")
    sql_refresh_locf_mv = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY 
        public.time_series_locf
        WITH DATA;
    """
    run_query(c, sql_refresh_locf_mv, db="timescale", commit=True)

    return True


# def refresh_hybrid_time_series_materialized_view():
#     """
#     Refresh the hybrid time series materialized view,
#     which contains more granular data for dates closer
#     to the present time.
#     """
#     # Requires owner privileges (must be run by "master" user, not "app_user")

#     sql_refresh_hybrid_mv = """
#         REFRESH MATERIALIZED VIEW CONCURRENTLY
#         public.time_series_mv
#         WITH DATA;
#     """
#     run_query(c, sql_refresh_hybrid_mv, db="timescale", commit=True)

#     return True


def get_latest_timestamp_in_locf_copy(c) -> datetime:
    """Get the most recent timestamp in public.time_series_locf_copy"""
    sql = """
        select max(timestamp_utc)
        from public.time_series_locf_copy
        where timestamp_utc >= now() - interval '7 days'
    """
    _, rows = run_query(c, sql, db="timescale", fetchall=True)

    if not rows or not isinstance(rows, list) or len(rows) == 0:
        return None

    return rows[0]["timestamp_utc"]


def get_and_insert_latest_values(c, after_this_date: datetime):
    """
    Get the latest values from the "last one carried forward" materialized view,
    which are not already in the regular "copied" table, and insert them.
    This should allow us to run continuous aggregates on the "regular copied table".
    """

    datetime_string = after_this_date.strftime("%Y-%m-%d %H:%M:%S")

    sql_get_insert_latest = f"""
    insert into public.time_series_locf_copy (
        timestamp_utc, gateway,
		spm, spm_egas, cgp, cgp_uno, dgp, dtp, hpu, hpe, ht, ht_egas, agft, mgp, ngp, agfm, agfn,
        e3m3_d, m3pd, hp_limit, msp, mprl_max, mprl_avg, mprl_min, pprl_max, pprl_avg, pprl_min,
        area_max, area_avg, area_min, pf_max, pf_avg, pf_min, hpt, hp_raising_avg, hp_lowering_avg,
        der_dtp_vpd, der_hp_vpd, der_suc_vpd, der_dis_vpd, der_dis_temp_vpd, gvf, stroke_speed_avg,
        fluid_rate_vpd, agf_dis_temp, agf_dis_temp_max, end_stop_avg_pveh, end_stop_counts, end_tap_avg_time, end_tap_counts,
        END_STOP_TIME,
        DER_OK_COUNTS,
        stroke_up_time, stroke_down_time,
        HYD_OIL_LVL, HYD_FILT_LIFE, HYD_OIL_LIFE,
        -- booleans below
		hyd, hyd_egas, warn1, warn1_egas, warn2, warn2_egas, mtr, mtr_egas, clr, clr_egas, htr, htr_egas, aux_egas, prs, sbf,
        -- new slave metrics
        STROKE_UP_TIME_SL,
        STROKE_DOWN_TIME_SL,
        SPM_EGAS_SL,
        STROKE_SPEED_AVG_SL,
        CGP_SL,
        DGP_SL,
        AGFN_SL,
        AGFM_SL,
        HP_RAISING_AVG_SL,
        HP_LOWERING_AVG_SL,
        AGFT_SL,
        MGP_SL,
        lag_btm_ms,
        lag_top_ms
    )
    select
        timestamp_utc, gateway,
		spm, spm_egas, cgp, cgp_uno, dgp, dtp, hpu, hpe, ht, ht_egas, agft, mgp, ngp, agfm, agfn,
        e3m3_d, m3pd, hp_limit, msp, mprl_max, mprl_avg, mprl_min, pprl_max, pprl_avg, pprl_min,
        area_max, area_avg, area_min, pf_max, pf_avg, pf_min, hpt, hp_raising_avg, hp_lowering_avg,
        der_dtp_vpd, der_hp_vpd, der_suc_vpd, der_dis_vpd, der_dis_temp_vpd, gvf, stroke_speed_avg,
        fluid_rate_vpd, agf_dis_temp, agf_dis_temp_max, end_stop_avg_pveh, end_stop_counts, end_tap_avg_time, end_tap_counts,
        END_STOP_TIME,
        DER_OK_COUNTS,
        stroke_up_time, stroke_down_time,
        HYD_OIL_LVL, HYD_FILT_LIFE, HYD_OIL_LIFE,
        -- booleans below
		hyd, hyd_egas, warn1, warn1_egas, warn2, warn2_egas, mtr, mtr_egas, clr, clr_egas, htr, htr_egas, aux_egas, prs, sbf,
        -- new slave metrics
        STROKE_UP_TIME_SL,
        STROKE_DOWN_TIME_SL,
        SPM_EGAS_SL,
        STROKE_SPEED_AVG_SL,
        CGP_SL,
        DGP_SL,
        AGFN_SL,
        AGFM_SL,
        HP_RAISING_AVG_SL,
        HP_LOWERING_AVG_SL,
        AGFT_SL,
        MGP_SL,
        lag_btm_ms,
        lag_top_ms
	FROM public.time_series_locf
    where timestamp_utc > '{datetime_string}'
    """
    run_query(c, sql_get_insert_latest, db="timescale", commit=True)

    return True


def force_refresh_continuous_aggregates(c, after_this_date: datetime):
    """
    Force the continuous aggregates to refresh with the latest data.
    It is possible to specify NULL in a manual refresh to get an open-ended range,
    but we do not recommend using it, because you could inadvertently materialize
    a large amount of data, slow down your performance, and have unintended consequences
    on other policies like data retention.
    """

    def get_sql(
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

        return f"CALL refresh_continuous_aggregate('{name}', '{dt_beg_str}', '{dt_end_str}');"

    views_to_update = {
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
            sql = get_sql(view, date_begin=after_this_date, min_window=min_time_delta)
            # AUTOCOMMIT is set, so commit is irrelevant
            run_query(c, sql, db="timescale", commit=False, conn=conn)
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


@error_wrapper()
def main(c):
    """Main entrypoint function"""

    exit_if_already_running(c, pathlib.Path(__file__).name)

    # First refresh the main "last one carried forward" MV
    refresh_locf_materialized_view(c)

    # # Refresh the hybrid MV with different granularities by date
    # refresh_hybrid_time_series_materialized_view()

    # Get the lastest values from the LOCF MV and insert
    # into the regular table, to trigger the continuous aggregates to refresh
    timestamp = get_latest_timestamp_in_locf_copy(c)
    get_and_insert_latest_values(c, after_this_date=timestamp)

    # Force the continuous aggregates to refresh
    force_refresh_continuous_aggregates(c, after_this_date=timestamp)

    return True


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
