import logging
import pathlib
import sys

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


def get_and_insert_latest_values(c):
    """
    Get the latest values from the "last one carried forward" materialized view,
    which are not already in the regular "copied" table, and insert them.
    This should allow us to run continuous aggregates on the "regular copied table".
    """

    sql_get_insert_latest = """
    --Get latest timestamp_utc and use it in the next query
    with latest_ts_utc as (
        select timestamp_utc
        from public.time_series_locf_copy
        order by timestamp_utc DESC
        limit 1
    )

    --Use the "latest_ts_utc" table from above as a filter
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
        HP_LOWERING_AVG_SL
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
        HP_LOWERING_AVG_SL
	FROM public.time_series_locf
    --Use the "latest_ts_utc" table from above as a filter
    where timestamp_utc > (select timestamp_utc from latest_ts_utc)
    """
    run_query(c, sql_get_insert_latest, db="timescale", commit=True)

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
    get_and_insert_latest_values(c)

    return True


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
