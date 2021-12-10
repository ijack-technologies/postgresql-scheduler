#!/bin/bash

# First setup the cron job to run some database refresh jobs
# e.g. alarm log materialized view
/bin/bash /cron_d/cron_setup.sh
# nohup python3 /cron_d/scheduled_jobs.py &

# Ah, ha, ha, ha, stayin' alive...
# https://github.com/docker/compose/issues/1926
while :; do :; done & kill -STOP $! && wait $!
