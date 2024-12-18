from pathlib import Path

from project.utils import (
    Config,
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

    run_query(sql=SQL, db="timescale", commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    main(c)
