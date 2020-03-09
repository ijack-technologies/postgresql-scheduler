#!/bin/bash

echo "Docker container has been started"
echo "Setting up the cron scheduler..."

# To make the Docker container environment variables available to cron.
# declare is a builtin command of the bash shell. It is used to declare 
# shell variables and functions, set their attributes and display their values.
# Without this step, the environment variables will not be available in cron!
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

# Send the following crontab to crontab.txt
touch /crontab.txt
echo "SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
BASH_ENV=/container.env

# Clean up the logs older than 7 days, and truncate the cron.log file
*/5 * * * * python3 /cron.d/alarm_log_mv_refresh.py >> /var/log/cron.log 2>&1
# Leave the last line blank for a valid cron file" > /crontab.txt

# Make the shell scripts executable
# chmod +x /project/cron/truncate_logs.sh

# Set the default crontab as the crontab.txt file
crontab /crontab.txt

# Run the cron process, so its logs will be visible to docker logs
# cron -f 
nohup cron && tail -F /var/log/cron.log 2>&1 &
