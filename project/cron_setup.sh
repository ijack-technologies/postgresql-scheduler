#!/bin/bash

echo "Docker container has been started"
echo "Setting up the cron scheduler..."

# To make the Docker container environment variables available to cron.
# declare is a builtin command of the bash shell. It is used to declare
# shell variables and functions, set their attributes and display their values.
# Without this step, the environment variables will not be available in cron!
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

# Send the following crontab to /project/crontab.txt
touch /project/crontab.txt
echo "SHELL=/bin/bash
PATH=/:/project:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
BASH_ENV=/container.env

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
# Leave the last line blank for a valid cron file" > /project/crontab.txt

# Make the shell scripts executable
# chmod +x /project/cron/truncate_logs.sh

# Set the default crontab as the /project/crontab.txt file
crontab /project/crontab.txt

# Run the cron process, so its logs will be visible to docker logs
# cron -f

FILE=/project/logs/cron.log
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "$FILE does not exist. Creating it now..."
    touch /project/logs/cron.log
fi

nohup cron && tail -F $FILE 2>&1 &
