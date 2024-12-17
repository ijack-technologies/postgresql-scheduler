"""
This just restarts the TimescaleDB background workers once a day,
in case they've stopped, for whatever reason
"""

from pathlib import Path

from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

LOGFILE_NAME = "timescaledb_restart_background_workers"


def restart_background_workers_timescaledb(c) -> bool:
    """Restart the TimescaleDB background workers"""

    SQL = "SELECT _timescaledb_internal.start_background_workers();"
    run_query(c, SQL, db="timescale", commit=True)

    return True


@error_wrapper()
def main(c: Config) -> bool:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    is_good = restart_background_workers_timescaledb(c)

    return is_good


if __name__ == "__main__":
    c = Config()

    main(c)
