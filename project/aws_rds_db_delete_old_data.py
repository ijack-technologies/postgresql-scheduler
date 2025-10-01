from pathlib import Path

from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

# Delete old data
SQL = """
    delete 
    FROM public.error_logs
    WHERE timestamp_utc < now() - interval '1 month'
;
"""


@error_wrapper(filename=Path(__file__).name)
def main(c: Config) -> None:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    run_query(sql=SQL, db="aws_rds", commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    main(c)
