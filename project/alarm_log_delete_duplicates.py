from pathlib import Path

from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    run_query,
)

# Delete duplicate records once a day (this table gets lots of duplicates if left alone)
SQL = """
    delete 
    FROM public.alarm_log
    WHERE timestamp_utc_inserted IN (
        SELECT timestamp_utc_inserted
        FROM (
            SELECT timestamp_utc_inserted,
            ROW_NUMBER() OVER ( 
                PARTITION BY timestamp_local, power_unit, abbrev, value
                ORDER BY timestamp_utc_inserted 
            ) AS row_num
            FROM public.alarm_log 
        ) t
        WHERE t.row_num > 1 
    );
"""


@error_wrapper(filename=Path(__file__).name)
def main(c: Config) -> None:
    """Main entrypoint function"""
    global SQL

    exit_if_already_running(c, Path(__file__).name)

    run_query(SQL, commit=True)

    return None


if __name__ == "__main__":
    c = Config()
    main(c)
