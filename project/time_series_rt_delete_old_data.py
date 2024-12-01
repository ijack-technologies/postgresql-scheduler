import sys
from pathlib import Path

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project.utils import (
    Config,
    configure_logging,
    error_wrapper,
    exit_if_already_running,
    run_query,
)


# Delete old data from the time_series_rt table
SQL = """
    delete 
    FROM public.time_series_rt
    WHERE timestamp_utc < now() - interval '1 year'
;
"""


@error_wrapper()
def main(c: Config) -> None:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    run_query(c=c, sql=SQL, db="timescale", commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    c.logger = configure_logging(
        __name__, logfile_name="time_series_rt_delete_old_data"
    )
    main(c)
