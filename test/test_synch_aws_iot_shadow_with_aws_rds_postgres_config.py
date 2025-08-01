# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project import synch_aws_iot_shadow_with_aws_rds_postgres_config
from project.utils import Config

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


LOGFILE_NAME = "test_synch_aws_iot_shadow_with_aws_rds_postgres_config"

c = Config()
c.DEV_TEST_PRD = "development"


class TestAll(unittest.TestCase):
    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    def test_synch_aws_iot_shadow_with_aws_rds_postgres_config(self):
        """Test the main program"""
        global c
        synch_aws_iot_shadow_with_aws_rds_postgres_config.main(c)


if __name__ == "__main__":
    unittest.main()
