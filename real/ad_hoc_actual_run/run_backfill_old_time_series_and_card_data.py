# # # Load the secret environment variables using python-dotenv
# # from dotenv import load_dotenv
# # load_dotenv()

# import sys
# import unittest
# from unittest.mock import patch

# # Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
# def insert_path(pythonpath):
#     try:
#         sys.path.index(pythonpath)
#     except ValueError:
#         sys.path.insert(0, pythonpath)

# insert_path("/workspace/ad_hoc")
# insert_path("/workspace/project")

# # local imports
# from project.utils import Config
# from project.logger_config import configure_logging
# import backfill_old_time_series_and_card_data


# class TestAll(unittest.TestCase):

#     # # executed after each test
#     # def tearDown(self):
#     #     pass

#     # executed prior to each test below, not just when the class is initialized
#     def setUp(self):
#         global c
#         c.DEV_TEST_PRD = "development"
#         c.TEST_FUNC = True

#     def test_gateways_mv_refresh(self):
#         """Test the main program"""
#         global c

#         # This is no longer a materialized view--just a regular view
#         # with patch("backfill_old_time_series_and_card_data.exit_if_already_running") as _:
#         backfill_old_time_series_and_card_data.main(c)


# if __name__ == "__main__":

# LOGFILE_NAME = "test_backfill_old_time_series_and_card_data"

# c = Config()
# c.DEV_TEST_PRD = "development"
# configure_logging(
#     __name__, logfile_name=LOGFILE_NAME
# )
#     unittest.main()
