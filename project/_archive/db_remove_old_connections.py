from pathlib import Path

from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

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
            current_timestamp - state_change > interval '6 hours' 
    )
    SELECT
        pg_terminate_backend(pid)
    FROM
        inactive_connections 
    WHERE
        rank > 1 -- Leave one connection for each application connected to the database
"""


@error_wrapper(filename=Path(__file__).name)
def main(c):
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    # Run this for the main IJACK AWS RDS database to clean up unused connections
    run_query(SQL, db="ijack", commit=True)

    # If we run the following for TimescaleDB, it causes errors,
    # and connection buildup doesn't seem to be a problem with TimescaleDB:
    """
        Here's the error information:
    Traceback (most recent call last):
    File "/inserter/inserter.py", line 338, in execute_sql
        cursor.execute(sql, values)
    psycopg2.errors.AdminShutdown: terminating connection due to administrator command
    SSL connection has been closed unexpectedly


    During handling of the above exception, another exception occurred:

    Traceback (most recent call last):
    File "/inserter/inserter.py", line 853, in main
        insert_alarm_log_rds(
    File "/inserter/inserter.py", line 599, in insert_alarm_log_rds
        rc = execute_sql(
    File "/inserter/inserter.py", line 345, in execute_sql
        conn.rollback()
    psycopg2.InterfaceError: connection already closed
    """
    # run_query(SQL, db="timescale", commit=True)

    return None


if __name__ == "__main__":
    c = Config()

    main(c)
