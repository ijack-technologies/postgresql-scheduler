import logging
import pathlib

# local imports
from utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

LOG_LEVEL = logging.INFO
LOGFILE_NAME = "db_remove_old_connections"

SQL = """
    WITH inactive_connections AS (
        SELECT
            pid,
            rank() over (partition by client_addr order by backend_start ASC) as rank
        FROM 
            pg_stat_activity
        WHERE
            -- Exclude the thread owned connection (ie no auto-kill)
            pid <> pg_backend_pid( )
        AND
            -- Exclude known applications connections
            application_name !~ '(?:psql)|(?:pgAdmin.+)'
        AND
            -- Include connections to the same database the thread is connected to.
            -- 'ijack' is the current_database(). There's also 'odoo'
            datname = current_database()
        AND
            -- Include connections using the same thread username connection ('master')
            usename = current_user 
        AND
            -- Include inactive connections only
            state in ('idle', 'idle in transaction', 'idle in transaction (aborted)', 'disabled') 
        AND
            -- Include old connections (found with the state_change field)
            current_timestamp - state_change > interval '5 minutes' 
    )
    SELECT
        pg_terminate_backend(pid)
    FROM
        inactive_connections 
    WHERE
        rank > 1 -- Leave one connection for each application connected to the database
"""


@error_wrapper()
def main(c):
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, pathlib.Path(__file__).name)

    # Run this for both main databases
    run_query(c, SQL, db="ijack", commit=True)
    run_query(c, SQL, db="timescale", commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
    )
    main(c)
