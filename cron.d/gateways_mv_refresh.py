# import logging
# import pathlib

# # local imports
# from utils import (
#     Config,
#     configure_logging,
#     error_wrapper,
#     exit_if_already_running,
#     run_query,
# )

# LOG_LEVEL = logging.INFO
# LOGFILE_NAME = "gateways_mv_refresh"


# # Requires owner privileges (must be run by "master" user, not "app_user")
# # Note this one does NOT run concurrently because it doesn't have a unique index.
# # Some gateways serve two structures (e.g. dual XFERs), so they're duplicates re: power_unit field
# SQL = """
#     REFRESH MATERIALIZED VIEW
#     public.gateways
#     WITH DATA
# """


# @error_wrapper()
# def main(c):
#     """Main entrypoint function"""
#     global SQL

#     exit_if_already_running(c, pathlib.Path(__file__).name)

#     run_query(c, SQL, commit=True)

#     return None


# if __name__ == "__main__":
#     c = Config()
#     c.logger = configure_logging(
#         __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
#     )
#     main(c)
