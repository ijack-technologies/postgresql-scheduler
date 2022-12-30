#!/bin/bash

echo "Docker container has been started"
echo "Setting up the cron scheduler..."

# To make the Docker container environment variables available to cron.
# declare is a builtin command of the bash shell. It is used to declare 
# shell variables and functions, set their attributes and display their values.
# Without this step, the environment variables will not be available in cron!
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

# Send the following crontab to /cron_d/crontab.txt
touch /cron_d/crontab.txt
echo "SHELL=/bin/bash
PATH=/:/cron_d:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
BASH_ENV=/container.env

# min hour dom month dow   command
# */15 * * * * python3 /cron_d/_archive/db_remove_old_connections.py
# */3 * * * * python3 /cron_d/_archive/gateways_mv_refresh.py
# Delete duplicate alarm log records once daily
1 1 * * * python3 /cron_d/alarm_log_delete_duplicates.py
*/1 * * * * python3 /cron_d/alarm_log_mv_refresh.py
*/30 * * * * python3 /cron_d/time_series_mv_refresh.py
31 1 * * * python3 /cron_d/timescaledb_restart_background_workers.py
3 * * * * python3 /cron_d/synch_aws_iot_shadow_with_aws_rds_postgres_config.py
15 * * * * python3 /cron_d/update_gw_power_unit_id_from_shadow.py
# Leave the last line blank for a valid cron file" > /cron_d/crontab.txt

# Make the shell scripts executable
# chmod +x /project/cron/truncate_logs.sh

# Set the default crontab as the /cron_d/crontab.txt file
crontab /cron_d/crontab.txt

# Run the cron process, so its logs will be visible to docker logs
# cron -f 

FILE=/var/log/cron.log
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else 
    echo "$FILE does not exist. Creating it now..."
    touch /var/log/cron.log
fi

nohup cron && tail -F $FILE 2>&1 &
