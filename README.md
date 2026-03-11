# PostgreSQL Scheduler [DEPRECATED]

> **This repository is deprecated as of 2026-03-10.**
>
> All 11 scheduler jobs have been migrated into the [RCOM monorepo](https://github.com/ijack-technologies/rcom)
> under `packages/scheduler/`. The RCOM scheduler runs as a Docker Swarm sidecar container
> alongside the web application, sharing utilities (email, SMS, DB, IoT) instead of duplicating them.
>
> **PR**: https://github.com/ijack-technologies/rcom/pull/1664
>
> **What moved where:**
>
> | Old (this repo) | New (RCOM) |
> |---|---|
> | `project/scheduler_jobs.py` | `packages/scheduler/main.py` (APScheduler) |
> | `project/upload_bom_master_parts_to_db.py` | `packages/scheduler/jobs/bom_scraper.py` |
> | `project/time_series_mv_refresh.py` | `packages/scheduler/jobs/time_series_mv_refresh.py` |
> | `project/time_series_aggregate_calcs.py` | `packages/scheduler/jobs/time_series_aggregate_calcs.py` |
> | `project/time_series_rt_delete_old_data.py` | `packages/scheduler/jobs/time_series_rt_delete.py` |
> | `project/aws_rds_db_delete_old_data.py` | `packages/scheduler/jobs/rds_delete_old_data.py` |
> | `project/synch_aws_iot_shadow_*.py` | `packages/scheduler/jobs/iot_shadow_sync.py` |
> | `project/update_info_from_shadows.py` | `packages/scheduler/jobs/iot_shadow_update.py` |
> | `project/update_fx_exchange_rates_daily.py` | `packages/scheduler/jobs/fx_exchange_rates.py` |
> | `project/timescaledb_restart_background_workers.py` | `packages/scheduler/jobs/timescaledb_workers.py` |
> | `project/alerts_bulk_processor.py` | `packages/scheduler/jobs/alerts_bulk_processor.py` |
> | `project/monitor_disk_space.py` | `packages/scheduler/jobs/monitor_disk_space.py` |
> | `project/utils.py` | Split into `packages/shared/utils/{db_sync,sms_utils,aws_iot_utils,error_handling}.py` |
>
> **To decommission on EC2:**
> ```bash
> # Already scaled to 0 as of 2026-03-10
> docker service scale postgresql_scheduler_jobs=0 postgresql_scheduler_monitor=0
>
> # After verifying RCOM sidecar works (1-2 days):
> docker stack rm postgresql_scheduler
> ```

## Original Description

For running scheduled jobs on PostgreSQL databases like AWS RDS. PostgreSQL does not have its own scheduler built-in.
