# # # Load the secret environment variables using python-dotenv
# # from dotenv import load_dotenv
# # load_dotenv()

# import sys
# import unittest
# from unittest.mock import patch

# # Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
# def insert_path(pythonpath):
#     try:
#         sys.path.index(pythonpath)
#     except ValueError:
#         sys.path.insert(0, pythonpath)

# insert_path("/workspace/ad_hoc")
# insert_path("/workspace/project")

#
# from project.utils import Config
# import update_gps_lat_lon_from_land_locations


# class TestAll(unittest.TestCase):

#     # # executed after each test
#     # def tearDown(self):
#     #     pass

#     # executed prior to each test below, not just when the class is initialized
#     def setUp(self):
#         global c
#         c.DEV_TEST_PRD = "development"
#         c.TEST_FUNC = True

#     def test_update_info_from_shadows(self):
#         """Test the main program"""
#         global c
#         with patch(
#             "update_gps_lat_lon_from_land_locations.exit_if_already_running"
#         ) as _:
#             update_gps_lat_lon_from_land_locations.main(c)


# if __name__ == "__main__":

# LOGFILE_NAME = "test_main_programs"

# c = Config()
# c.DEV_TEST_PRD = "development"
#     unittest.main()
